/*
 * Copyright [2021] [EnginePlus Team]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.star.commands

import java.io.File

import com.engineplus.star.tables.StarTable
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.functions._
import org.apache.spark.sql.star.SnapshotManagement
import org.apache.spark.sql.star.sources.StarLakeSQLConf
import org.apache.spark.sql.star.test.StarLakeTestUtils
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.{AnalysisException, DataFrame, QueryTest, Row}
import org.apache.spark.util.Utils
import org.scalatest.BeforeAndAfterEach

class UpsertSuiteBase extends QueryTest
  with SharedSparkSession with BeforeAndAfterEach
  with StarLakeTestUtils {

  import testImplicits._

  var tempDir: File = _

  var snapshotManagement: SnapshotManagement = _

  protected def tempPath = tempDir.getCanonicalPath

  protected def readStarLakeTable(path: String): DataFrame = {
    spark.read.format("star").load(path)
  }

  override def beforeEach() {
    super.beforeEach()
    tempDir = Utils.createTempDir()
    snapshotManagement = SnapshotManagement(new Path(tempPath))
  }

  override def afterEach() {
    try {
      Utils.deleteRecursively(tempDir)
      try {
        snapshotManagement.updateSnapshot()
        StarTable.forPath(snapshotManagement.table_name).dropTable()
      } catch {
        case e: Exception =>
      }
    } finally {
      super.afterEach()
    }
  }

  //  protected def executeUpsert(df: DataFrame, condition: Option[String], tableName: String): Unit
  protected def executeUpsert(df: DataFrame, condition: Option[String], tableName: String): Unit = {
    if (condition.isEmpty) {
      StarTable.forPath(tableName)
        .upsert(df)
    } else {
      StarTable.forPath(tableName)
        .upsert(df, condition.get)
    }
  }

  protected def initTable(df: DataFrame,
                          rangePartition: Seq[String] = Nil,
                          hashPartition: Seq[String] = Nil,
                          hashBucketNum: Int = 2): Unit = {
    val writer = df.write.format("star").mode("overwrite")

    writer
      .option("rangePartitions", rangePartition.mkString(","))
      .option("hashPartitions", hashPartition.mkString(","))
      .option("hashBucketNum", hashBucketNum)
      .save(snapshotManagement.table_name)
  }

  implicit def jsonStringToSeq(json: String): Seq[String] = json.split("\n")

  protected def checkUpsert(df: DataFrame,
                            condition: Option[String],
                            expectedResults: Seq[Row],
                            colNames: Seq[String],
                            tableName: Option[String] = None): Unit = {
    executeUpsert(df, condition, tableName.getOrElse(tempPath))
    checkAnswer(readStarLakeTable(tempPath).select(colNames.map(col): _*), expectedResults)
  }

  protected def checkBigDataUpsert(df: DataFrame,
                                   condition: Option[String],
                                   expectedResults: DataFrame,
                                   colNames: Seq[String],
                                   tableName: Option[String] = None): Unit = {
    executeUpsert(df, condition, tableName.getOrElse(tempPath))
    val starData = readStarLakeTable(tempPath).select(colNames.map(col): _*).rdd.persist()
    val expectedData = expectedResults.rdd.persist()
    val firstDiff = expectedData.subtract(starData).persist()
    val secondDiff = starData.subtract(expectedData).persist()
    assert(firstDiff.count() == 0)
    assert(secondDiff.count() == 0)
  }

  protected def checkUpsertByFilter(df: DataFrame,
                                    condition: Option[String],
                                    expectedResults: Seq[Row],
                                    filter: String,
                                    colNames: Seq[String],
                                    tableName: Option[String] = None): Unit = {
    executeUpsert(df, condition, tableName.getOrElse(tempPath))

    val starDF = readStarLakeTable(tempPath)
      .filter(filter)
      .select(colNames.map(col): _*)
      .persist()
    starDF.show()
    checkAnswer(starDF, expectedResults)
  }


  protected def checkUpsertBySelect(df: DataFrame,
                                    condition: Option[String],
                                    expectedResults: Seq[Row],
                                    selectCols: String,
                                    tableName: Option[String] = None): Unit = {
    executeUpsert(df, condition, tableName.getOrElse(tempPath))

    val starDF = readStarLakeTable(tempPath)
      .select(selectCols.split(",").map(col): _*).persist()
    checkAnswer(starDF, expectedResults)
  }


  test("merge - same column") {
    initTable(
      Seq((20201101, 1, 1), (20201101, 2, 2), (20201101, 3, 3), (20201102, 4, 4))
        .toDF("range", "hash", "value"),
      "range",
      "hash")

    checkUpsert(
      Seq((20201101, 1, 11), (20201101, 3, 33), (20201101, 4, 44))
        .toDF("range", "hash", "value"),
      None,
      Row(20201101, 1, 11) :: Row(20201101, 2, 2) :: Row(20201101, 3, 33) :: Row(20201101, 4, 44) :: Row(20201102, 4, 4) :: Nil,
      Seq("range", "hash", "value"))
  }


  test("merge - different columns") {
    initTable(
      Seq((20201101, 1, 1), (20201101, 2, 2), (20201101, 3, 3), (20201102, 4, 4))
        .toDF("range", "hash", "value"),
      "range",
      "hash")

    withSQLConf(StarLakeSQLConf.SCHEMA_AUTO_MIGRATE.key -> "true") {
      checkUpsert(
        Seq((20201101, 1, 11), (20201101, 3, 33), (20201101, 4, 44))
          .toDF("range", "hash", "name"),
        None,
        Row(20201101, 1, 1, 11) :: Row(20201101, 2, 2, null) :: Row(20201101, 3, 3, 33) :: Row(20201101, 4, null, 44) :: Row(20201102, 4, 4, null) :: Nil,
        Seq("range", "hash", "value", "name"))
    }
  }


  test("merge one file with empty batch") {
    initTable(
      Seq((20201101, 1, 1), (20201101, 2, 2), (20201102, 3, 3), (20201102, 4, 4))
        .toDF("range", "hash", "value"),
      "range",
      "hash")

    checkAnswer(readStarLakeTable(tempPath)
      .filter("value < 3")
      .select("range", "hash", "value"),
      Seq((20201101, 1, 1), (20201101, 2, 2))
        .toDF("range", "hash", "value")
    )
  }


  test("merge multi files with empty batch") {
    initTable(
      Seq((20201101, 1, 1), (20201101, 2, 2), (20201101, 3, 3), (20201102, 4, 4), (20201102, 1, 1))
        .toDF("range", "hash", "value"),
      "range",
      "hash")

    executeUpsert(Seq((20201102, 4, 5))
      .toDF("range", "hash", "value"),
      None,
      tempPath)

    checkAnswer(readStarLakeTable(tempPath)
      .filter("value < 3")
      .select("range", "hash", "value"),
      Seq((20201101, 1, 1), (20201101, 2, 2), (20201102, 1, 1))
        .toDF("range", "hash", "value")
    )
  }

  test("basic upsert - same columns") {
    initTable(
      Seq((20201101, 1, 1), (20201101, 2, 2), (20201101, 3, 3), (20201102, 4, 4))
        .toDF("range", "hash", "value"),
      "range",
      "hash")

    checkAnswer(readStarLakeTable(tempPath).select("range", "hash", "value"),
      Row(20201101, 1, 1) :: Row(20201101, 2, 2) :: Row(20201101, 3, 3) :: Row(20201102, 4, 4) :: Nil)

    val e = intercept[AnalysisException] {
      withSQLConf(StarLakeSQLConf.USE_DELTA_FILE.key -> "false") {
        checkUpsert(
          Seq((20201101, 1, 11), (20201101, 3, 33), (20201101, 4, 44))
            .toDF("range", "hash", "value"),
          None,
          Row(20201101, 1, 1, 11) :: Row(20201101, 2, 2, null) :: Row(20201101, 3, 3, 33) :: Row(20201101, 4, null, 44) :: Row(20201102, 4, 4, null) :: Nil,
          Seq("range", "hash", "value"))
      }
    }

    assert(e.getMessage().contains("Some condition for range partition should be declared"))

    checkUpsert(
      Seq((20201101, 1, 11), (20201101, 3, 33), (20201101, 4, 44))
        .toDF("range", "hash", "value"),
      None,
      Row(20201101, 1, 11) :: Row(20201101, 2, 2) :: Row(20201101, 3, 33) :: Row(20201101, 4, 44) :: Row(20201102, 4, 4) :: Nil,
      Seq("range", "hash", "value"))
  }


  test("basic upsert - different columns") {
    initTable(
      Seq((20201101, 1, 1), (20201101, 2, 2), (20201101, 3, 3), (20201102, 4, 4))
        .toDF("range", "hash", "value"),
      "range",
      "hash")

    //
    val e = intercept[AnalysisException] {
      checkUpsert(
        Seq((20201101, 1, 11), (20201101, 3, 33), (20201101, 4, 44))
          .toDF("range", "hash", "name"),
        None,
        Row(20201101, 1, 1, 11) :: Row(20201101, 2, 2, null) :: Row(20201101, 3, 3, 33) :: Row(20201101, 4, null, 44) :: Row(20201102, 4, 4, null) :: Nil,
        Seq("range", "hash", "value", "name"))
    }
    assert(e.getMessage().contains("Can't find column"))

    withSQLConf(StarLakeSQLConf.SCHEMA_AUTO_MIGRATE.key -> "true") {
      checkUpsert(
        Seq((20201101, 1, 11), (20201101, 3, 33), (20201101, 4, 44))
          .toDF("range", "hash", "name"),
        None,
        Row(20201101, 1, 1, 11) :: Row(20201101, 2, 2, null) :: Row(20201101, 3, 3, 33) :: Row(20201101, 4, null, 44) :: Row(20201102, 4, 4, null) :: Nil,
        Seq("range", "hash", "value", "name"))
    }
  }

  test("should failed to upsert external columns when SCHEMA_AUTO_MIGRATE is false") {
    initTable(
      Seq((20201101, 1, 1), (20201101, 2, 2), (20201101, 3, 3), (20201102, 4, 4))
        .toDF("range", "hash", "value"),
      "range",
      "hash")

    val e = intercept[AnalysisException] {
      withSQLConf(StarLakeSQLConf.SCHEMA_AUTO_MIGRATE.key -> "false") {
        checkUpsert(
          Seq((20201101, 1, 11), (20201101, 3, 33), (20201101, 4, 44))
            .toDF("range", "hash", "name"),
          None,
          Row(20201101, 1, 1, 11) :: Row(20201101, 2, 2, null) :: Row(20201101, 3, 3, 33) :: Row(20201101, 4, null, 44) :: Row(20201102, 4, 4, null) :: Nil,
          Seq("range", "hash", "value"))
      }
    }
    assert(e.getMessage().contains("Can't find column"))

  }


  test("upsert in new table should failed") {
    val e = intercept[AnalysisException] {
      checkUpsert(
        Seq((20201101, 1, 11), (20201101, 3, 33), (20201101, 4, 44))
          .toDF("range", "hash", "value"),
        None,
        Row(20201101, 1, 11) :: Row(20201101, 3, 33) :: Row(20201101, 4, 44) :: Nil,
        Seq("range", "hash", "value"))
    }
    assert(e.getMessage().contains("doesn't exist"))
  }


  test("upsert - can't use delta file") {
    initTable(
      Seq((20201101, 1, 1), (20201101, 2, 2), (20201101, 3, 3), (20201102, 4, 4))
        .toDF("range", "hash", "value"),
      "range",
      "hash")

    //should failed when condition is null
    val e1 = intercept[AnalysisException] {
      withSQLConf(
        StarLakeSQLConf.USE_DELTA_FILE.key -> "false",
        StarLakeSQLConf.ALLOW_FULL_TABLE_UPSERT.key -> "false") {
        checkUpsert(
          Seq((20201101, 1, 11), (20201101, 3, 33), (20201101, 4, 44))
            .toDF("range", "hash", "value"),
          None,
          Row(20201101, 1, 1, 11) :: Row(20201101, 2, 2, null) :: Row(20201101, 3, 3, 33) :: Row(20201101, 4, null, 44) :: Row(20201102, 4, 4, null) :: Nil,
          Seq("range", "hash", "value"))
      }
    }
    assert(e1.getMessage().contains("Some condition for range partition should be declared to prevent full table scan when upsert"))

    withSQLConf(
      StarLakeSQLConf.USE_DELTA_FILE.key -> "false",
      StarLakeSQLConf.ALLOW_FULL_TABLE_UPSERT.key -> "true") {
      checkUpsert(
        Seq((20201101, 1, 11), (20201101, 3, 33), (20201101, 4, 44))
          .toDF("range", "hash", "value"),
        None,
        Row(20201101, 1, 11) :: Row(20201101, 2, 2) :: Row(20201101, 3, 33) :: Row(20201101, 4, 44) :: Row(20201102, 4, 4) :: Nil,
        Seq("range", "hash", "value"))
    }

  }

  test("upsert without range partitions") {
    initTable(
      Seq((20201101, 1, 1), (20201101, 2, 2), (20201101, 3, 3), (20201101, 4, 4))
        .toDF("range", "hash", "value"),
      "",
      "hash")

    checkUpsert(
      Seq((20201101, 1, 11), (20201101, 3, 33), (20201102, 4, 44))
        .toDF("range", "hash", "value"),
      None,
      Row(20201101, 1, 11) :: Row(20201101, 2, 2) :: Row(20201101, 3, 33) :: Row(20201102, 4, 44) :: Nil,
      Seq("range", "hash", "value"))
  }

  test("upsert without hash partitions - should fail") {
    initTable(
      Seq((20201101, 1, 1), (20201101, 2, 2), (20201101, 3, 3), (20201102, 4, 4))
        .toDF("range", "hash", "value"),
      "range",
      "")

    val e = intercept[AnalysisException] {
      checkUpsert(
        Seq((20201101, 1, 11), (20201101, 3, 33), (20201101, 4, 44))
          .toDF("range", "hash", "value"),
        None,
        Row(20201101, 1, 11) :: Row(20201101, 2, 2) :: Row(20201101, 3, 33) :: Row(20201101, 4, 44) :: Row(20201102, 4, 4) :: Nil,
        Seq("range", "hash", "value"))
    }
    assert(e.getMessage().contains("Table should define Hash partition column to use upsert"))
  }


  test("upsert with multiple range and hash partitions") {
    initTable(
      Seq((20201101, 1, 1, 1, 1), (20201101, 2, 2, 2, 2), (20201101, 3, 3, 3, 3), (20201102, 4, 4, 4, 4))
        .toDF("range1", "range2", "hash1", "hash2", "value"),
      "range1,range2",
      "hash1,hash2")

    checkUpsert(
      Seq((20201101, 1, 1, 1, 11), (20201101, 3, 3, 3, 33), (20201101, 4, 4, 4, 44))
        .toDF("range1", "range2", "hash1", "hash2", "value"),
      None,
      Row(20201101, 1, 1, 1, 11) :: Row(20201101, 2, 2, 2, 2) :: Row(20201101, 3, 3, 3, 33) :: Row(20201101, 4, 4, 4, 44) :: Row(20201102, 4, 4, 4, 4) :: Nil,
      Seq("range1", "range2", "hash1", "hash2", "value"))
  }

  test("source dataFrame without partition columns") {
    initTable(
      Seq((20201101, 1, 1, 1, 1), (20201101, 2, 2, 2, 2), (20201101, 3, 3, 3, 3), (20201102, 4, 4, 4, 4))
        .toDF("range1", "range2", "hash1", "hash2", "value"),
      "range1,range2",
      "hash1,hash2")

    val e1 = intercept[AnalysisException] {
      checkUpsert(
        Seq((20201101, 1, 1, 11), (20201101, 3, 3, 33), (20201101, 4, 4, 44))
          .toDF("range1", "hash1", "hash2", "value"),
        None,
        Row(20201101, 1, 1, 1, 11) :: Row(20201101, 2, 2, 2, 2) :: Row(20201101, 3, 3, 3, 33) :: Row(20201101, 4, 4, 4, 44) :: Row(20201102, 4, 4, 4, 4) :: Nil,
        Seq("range1", "range2", "hash1", "hash2", "value"))
    }
    assert(e1.getMessage().contains("Couldn't find all the partition columns"))

    val e2 = intercept[AnalysisException] {
      checkUpsert(
        Seq((20201101, 1, 1, 11), (20201101, 3, 3, 33), (20201101, 4, 4, 44))
          .toDF("range1", "range2", "hash2", "value"),
        None,
        Row(20201101, 1, 1, 1, 11) :: Row(20201101, 2, 2, 2, 2) :: Row(20201101, 3, 3, 3, 33) :: Row(20201101, 4, 4, 4, 44) :: Row(20201102, 4, 4, 4, 4) :: Nil,
        Seq("range1", "range2", "hash1", "hash2", "value"))
    }
    assert(e2.getMessage().contains("Couldn't find all the partition columns"))
  }


  test("upsert with condition") {
    withSQLConf(StarLakeSQLConf.USE_DELTA_FILE.key -> "false") {
      initTable(
        Seq((20201101, 1, 1), (20201101, 2, 2), (20201101, 3, 3), (20201102, 4, 4))
          .toDF("range", "hash", "value"),
        "range",
        "hash")

      //should filed when condition is null
      val e1 = intercept[AnalysisException] {
        checkUpsert(
          Seq((20201101, 1, 11), (20201101, 3, 33), (20201101, 4, 44))
            .toDF("range", "hash", "value"),
          None,
          Row(20201101, 1, 11) :: Row(20201101, 2, 2) :: Row(20201101, 3, 33) :: Row(20201101, 4, 44) :: Row(20201102, 4, 4) :: Nil,
          Seq("range", "hash", "value"))
      }
      assert(e1.getMessage().contains("Some condition for range partition should be declared"))

      checkUpsert(
        Seq((20201101, 1, 11), (20201101, 3, 33), (20201101, 4, 44))
          .toDF("range", "hash", "value"),
        Option("range=20201101"),
        Row(20201101, 1, 11) :: Row(20201101, 2, 2) :: Row(20201101, 3, 33) :: Row(20201101, 4, 44) :: Row(20201102, 4, 4) :: Nil,
        Seq("range", "hash", "value"))


    }
  }

  Range(1, 3).foreach(i => {
    test("filter requested columns, upsert time: " + i) {
      initTable(
        Seq((20201101, 1, 1, 1, 1), (20201101, 2, 2, 2, 2), (20201101, 3, 3, 3, 3), (20201101, 4, 4, 4, 4))
          .toDF("range", "hash", "value", "name", "age"),
        "range",
        "hash")

      i match {
        case 1 =>
          checkUpsertByFilter(
            Seq((20201102, 1, 11), (20201102, 3, 33), (20201102, 4, 44))
              .toDF("range", "hash", "value"),
            None,
            Row(20201102, 1, 11, null, null) :: Row(20201102, 3, 33, null, null) :: Row(20201102, 4, 44, null, null) :: Nil,
            "range=20201102",
            Seq("range", "hash", "value", "name", "age"))

        case 2 =>
          executeUpsert(Seq((20201102, 1, 11), (20201102, 3, 33), (20201102, 4, 44))
            .toDF("range", "hash", "value"),
            None,
            tempPath)
          checkUpsertByFilter(
            Seq((20201102, 1, 111, 11), (20201102, 2, 222, 22), (20201102, 3, 333, 33))
              .toDF("range", "hash", "value", "name"),
            None,
            Row(20201102, 1, 111, 11, null) :: Row(20201102, 2, 222, 22, null) :: Row(20201102, 3, 333, 33, null) :: Row(20201102, 4, 44, null, null) :: Nil,
            "range=20201102",
            Seq("range", "hash", "value", "name", "age"))

        case 3 =>
          executeUpsert(Seq((20201102, 1, 111, 11), (20201102, 2, 222, 22), (20201102, 3, 333, 33))
            .toDF("range", "hash", "value", "name"),
            None,
            tempPath)
          executeUpsert(Seq((20201102, 1, 11), (20201102, 3, 33), (20201102, 4, 44))
            .toDF("range", "hash", "value"),
            None,
            tempPath)
          checkUpsertByFilter(
            Seq((20201102, 1, 111, 11), (20201102, 3, 333, 33))
              .toDF("range", "hash", "age", "name")
              .withColumn("value", lit(null)),
            None,
            Row(20201102, 1, null, 11, 111) :: Row(20201102, 2, 222, 22, null) :: Row(20201102, 3, null, 33, 333) :: Row(20201102, 4, 44, null, null) :: Nil,
            "range=20201102",
            Seq("range", "hash", "value", "name", "age"))

      }
    }
  })

  Range(1, 3).foreach(i => {
    test("select requested columns without hash columns, upsert time: " + i) {
      initTable(
        Seq((20201101, 1, 1, 1, 1), (20201101, 2, 2, 2, 2))
          .toDF("range", "hash", "value", "name", "age"),
        "range",
        "hash")

      if (i == 1) {
        checkUpsertBySelect(
          Seq((20201102, 1, 11), (20201102, 3, 33), (20201102, 4, 44))
            .toDF("range", "hash", "value"),
          None,
          Row(1) :: Row(2) :: Row(null) :: Row(null) :: Row(null) :: Nil,
          "age"
        )

      }

      if (i == 2) {
        executeUpsert(Seq((20201102, 1, 11), (20201102, 3, 33), (20201102, 4, 44))
          .toDF("range", "hash", "value"),
          None,
          tempPath)

        checkUpsertBySelect(
          Seq((20201102, 1, 111, 11), (20201102, 2, 222, 22), (20201102, 3, 333, 33))
            .toDF("range", "hash", "value", "name"),
          None,
          Row(1) :: Row(2) :: Row(null) :: Row(null) :: Row(null) :: Row(null) :: Nil,
          "age"
        )
      }


    }

  })

  Range(1, 4).foreach(i => {
    test("derange hash key and data schema order - int type, upsert time: " + i) {

      initTable(
        Seq((20201101, 1, 1, 1, 1, 1), (20201101, 2, 2, 2, 2, 2))
          .toDF("range", "hash1", "hash2", "value", "name", "age"),
        "range",
        "hash1,hash2")

      if (i == 1) {
        checkUpsertByFilter(
          Seq((20201102, 1, 12, 1), (20201102, 3, 32, 3), (20201102, 4, 42, 4))
            .toDF("range", "hash1", "hash2", "value"),
          None,
          Row(20201102, 1, 12, 1, null, null) :: Row(20201102, 3, 32, 3, null, null) :: Row(20201102, 4, 42, 4, null, null) :: Nil,
          "range=20201102",
          Seq("range", "hash1", "hash2", "value", "name", "age"))
      }

      if (i == 2) {
        executeUpsert(Seq((20201102, 1, 12, 1), (20201102, 3, 32, 3), (20201102, 4, 42, 4))
          .toDF("range", "hash1", "hash2", "value"),
          None,
          tempPath)

        checkUpsertByFilter(
          Seq((20201102, 12, 11, 1), (20201102, 22, 22, 2), (20201102, 32, 33, 3))
            .toDF("range", "hash2", "name", "hash1"),
          None,
          Row(20201102, 1, 12, 1, 11, null) :: Row(20201102, 2, 22, null, 22, null) :: Row(20201102, 3, 32, 3, 33, null) :: Row(20201102, 4, 42, 4, null, null) :: Nil,
          "range=20201102",
          Seq("range", "hash1", "hash2", "value", "name", "age"))
      }

      if (i == 3) {
        executeUpsert(Seq((20201102, 1, 12, 1), (20201102, 3, 32, 3), (20201102, 4, 42, 4))
          .toDF("range", "hash1", "hash2", "value"),
          None,
          tempPath)
        executeUpsert(Seq((20201102, 12, 11, 1), (20201102, 22, 22, 2), (20201102, 32, 33, 3))
          .toDF("range", "hash2", "name", "hash1"),
          None,
          tempPath)

        checkUpsertByFilter(
          Seq((20201102, 4567, 42, 456, 4), (20201102, 2345, 22, 234, 2), (20201102, 3456, 32, 345, 3))
            .toDF("range", "age", "hash2", "name", "hash1"),
          None,
          Row(20201102, 1, 12, 1, 11, null) :: Row(20201102, 2, 22, null, 234, 2345) :: Row(20201102, 3, 32, 3, 345, 3456) :: Row(20201102, 4, 42, 4, 456, 4567) :: Nil,
          "range=20201102",
          Seq("range", "hash1", "hash2", "value", "name", "age"))
      }

    }
  })

  Range(1, 4).foreach(i => {
    test("derange hash key and data schema order - string type, upsert times: " + i) {
      initTable(
        Seq(("20201101", "1", "1", "1", "1", "1"), ("20201101", "2", "2", "2", "2", "2"))
          .toDF("range", "hash1", "hash2", "value", "name", "age"),
        "range",
        "hash1,hash2")

      if (i == 1) {
        checkUpsertByFilter(
          Seq(("20201102", "1", "12", "1"), ("20201102", "3", "32", "3"), ("20201102", "4", "42", "4"))
            .toDF("range", "hash1", "hash2", "value"),
          None,
          Row("20201102", "1", "12", "1", null, null) :: Row("20201102", "3", "32", "3", null, null) :: Row("20201102", "4", "42", "4", null, null) :: Nil,
          "range='20201102'",
          Seq("range", "hash1", "hash2", "value", "name", "age"))
      }

      if (i == 2) {
        executeUpsert(Seq(("20201102", "1", "12", "1"), ("20201102", "3", "32", "3"), ("20201102", "4", "42", "4"))
          .toDF("range", "hash1", "hash2", "value"),
          None,
          tempPath)

        checkUpsertByFilter(
          Seq(("20201102", "12", "11", "1"), ("20201102", "22", "22", "2"), ("20201102", "32", "33", "3"))
            .toDF("range", "hash2", "name", "hash1"),
          None,
          Row("20201102", "1", "12", "1", "11", null) :: Row("20201102", "2", "22", null, "22", null) :: Row("20201102", "3", "32", "3", "33", null) :: Row("20201102", "4", "42", "4", null, null) :: Nil,
          "range='20201102'",
          Seq("range", "hash1", "hash2", "value", "name", "age"))

      }

      if (i == 3) {
        executeUpsert(Seq(("20201102", "1", "12", "1"), ("20201102", "3", "32", "3"), ("20201102", "4", "42", "4"))
          .toDF("range", "hash1", "hash2", "value"),
          None,
          tempPath)

        executeUpsert(
          Seq(("20201102", "12", "11", "1"), ("20201102", "22", "22", "2"), ("20201102", "32", "33", "3"))
            .toDF("range", "hash2", "name", "hash1"),
          None,
          tempPath)

        checkUpsertByFilter(
          Seq(("20201102", "4567", "42", "456", "4"), ("20201102", "2345", "22", "234", "2"), ("20201102", "3456", "32", "345", "3"))
            .toDF("range", "age", "hash2", "name", "hash1"),
          None,
          Row("20201102", "1", "12", "1", "11", null) :: Row("20201102", "2", "22", null, "234", "2345") :: Row("20201102", "3", "32", "3", "345", "3456") :: Row("20201102", "4", "42", "4", "456", "4567") :: Nil,
          "range='20201102'",
          Seq("range", "hash1", "hash2", "value", "name", "age"))
      }

    }
  })


  test("create table with hash key disordered") {
    withTempDir(dir => {
      val tablePath = dir.getAbsolutePath
      val df1 = Seq(("range", "a1", 1, "a2", "a"), ("range", "b1", 2, "b2", "b"), ("range", "c1", 3, "c2", "c"))
        .toDF("range", "v1", "hash1", "v2", "hash2")

      val df2 = Seq(("range", 1, "a11", "a22", "a"), ("range", 2, "b11", "b22", "b"), ("range", 3, "c11", "c22", "c"))
        .toDF("range", "hash1", "v1", "v2", "hash2")
      val df3 = Seq(("range", "d1", 4, "d2", "d"), ("range", "b111", 2, "b222", "b"), ("range", "c111", 3, "c222", "c"))
        .toDF("range", "v1", "hash1", "v2", "hash2")

      df1.write.mode("overwrite")
        .format("star")
        .option("rangePartitions", "range")
        .option("hashPartitions", "hash1,hash2")
        .option("hashBucketNum", "2")
        .save(tablePath)

      val table = StarTable.forPath(tablePath)
      table.upsert(df2)
      table.upsert(df3)

      val requiredDF = Seq(
        ("range", "a11", 1, "a22", "a"),
        ("range", "b111", 2, "b222", "b"),
        ("range", "c111", 3, "c222", "c"),
        ("range", "d1", 4, "d2", "d"))
        .toDF("range", "v1", "hash1", "v2", "hash2")

      checkAnswer(
        table.toDF.select("range", "hash1", "hash2", "v1", "v2"),
        requiredDF.select("range", "hash1", "hash2", "v1", "v2"))

      checkAnswer(
        table.toDF.select("hash2", "v1", "v2"),
        requiredDF.select("hash2", "v1", "v2"))

      checkAnswer(
        table.toDF.select("v1", "v2"),
        requiredDF.select("v1", "v2"))

      checkAnswer(
        table.toDF.select("range", "v2"),
        requiredDF.select("range", "v2"))

      table.compaction()

      checkAnswer(
        table.toDF.select("range", "hash1", "hash2", "v1", "v2"),
        requiredDF.select("range", "hash1", "hash2", "v1", "v2"))


    })
  }


}
