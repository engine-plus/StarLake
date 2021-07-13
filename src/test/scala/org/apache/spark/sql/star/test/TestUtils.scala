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

package org.apache.spark.sql.star.test

import com.engineplus.star.tables.StarTable
import org.apache.spark.SparkConf
import org.apache.spark.sql.execution.datasources.v2.merge.parquet.batch.merge_operator.MergeOperator
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}

object TestUtils {

  def getSparkSession(sparkConf: SparkConf = new SparkConf()): SparkSession = {

    sparkConf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    sparkConf.set("spark.sql.extensions", "com.engineplus.star.sql.StarSparkSessionExtension")
    sparkConf.set("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.star.catalog.StarLakeCatalog")
    sparkConf.set("spark.engineplus.star.deltaFile.enabled", "true")
    sparkConf.set("spark.engineplus.star.schema.autoMerge.enabled", "true")
    sparkConf.set("spark.engineplus.star.deltaFile.max.num", "2")
    sparkConf.set("spark.engineplus.star.compaction.interval", "360")
    sparkConf.set("spark.engineplus.star.cleanup.interval", "360")
    sparkConf.set("spark.sql.shuffle.partitions", "10")
    sparkConf.set("spark.sql.streaming.schemaInference", "true")
    sparkConf.set("spark.engineplus.star.meta.streaming_info.timeout", "120000")
//    sparkConf.set("spark.engineplus.star.parquet.block.size", "20")

    sparkConf.set("spark.default.parallelism", "8")

    SparkSession
      .builder
      .appName("manual unit test")
      .config(sparkConf)
      .master("local")
      .getOrCreate()
  }

  def getData1(num: Int, onlyOne: Boolean = true): Seq[(Int, String, String)] = {
    var data = Seq.empty[(Int, String, String)]
    val max_id = num * 2
    val rand = scala.util.Random
    for (i <- 0 until num) {
      data = data :+ (
        rand.nextInt(max_id),
        getStr(5),
        getRangePartition(onlyOne))
    }
    data
  }

  def getData2(num: Int, onlyOne: Boolean = true): Seq[(Int, String, String, String)] = {
    var data = Seq.empty[(Int, String, String, String)]
    val max_id = num * 2
    val rand = scala.util.Random
    for (i <- 0 until num) {
      data = data :+ (
        rand.nextInt(max_id),
        getStr(5),
        getStr(5),
        getRangePartition(onlyOne))
    }
    data
  }

  def getData3(num: Int, onlyOne: Boolean = true): Seq[(Int, String, String, String, String)] = {
    var data = Seq.empty[(Int, String, String, String, String)]
    val max_id = num * 2
    val rand = scala.util.Random
    for (i <- 0 until num) {
      data = data :+ (
        rand.nextInt(max_id),
        getStr(5),
        getStr(5),
        getStr(5),
        getRangePartition(onlyOne))
    }
    data
  }


  def getDataNew(num: Int, onlyOne: Boolean = true): Seq[(Int, String, Int, String, Int, String)] = {
    var data = Seq.empty[(Int, String, Int, String, Int, String)]
    val max_id = num * 2
    val rand = scala.util.Random
    for (i <- 0 until num) {
      data = data :+ (
        rand.nextInt(max_id),
        getStr(5),
        rand.nextInt(35),
        getStr(5),
        rand.nextInt(35),
        getRangePartition(onlyOne))
    }
    data
  }

  def getRangePartition(onlyOne: Boolean): String = {
    if (onlyOne) {
      "range1"
    } else {
      scala.util.Random.nextInt(3) match {
        case 1 => "range1"
        case _ => "range2"
      }
    }
  }


  def getStr(num: Int): String ={
    val rand = scala.util.Random
    (0 until num).map(n => rand.alphanumeric.head).mkString("")
  }

  def initTable(table_name: String,
                df: DataFrame,
                rangePartition: String,
                hashPartition: String,
                hashBucketNum: Int = 2): Unit = {
    val writer = df.write.format("star").mode("overwrite")

    writer
      .option("rangePartitions", rangePartition)
      .option("hashPartitions", hashPartition)
      .option("hashBucketNum", hashBucketNum)
      .save(table_name)
  }

  private def executeUpsert(tableName: String,
                            df: DataFrame,
                            condition: Option[String]): Unit = {
    if (condition.isEmpty) {
      StarTable.forPath(tableName)
        .upsert(df)
    } else {
      StarTable.forPath(tableName)
        .upsert(df, condition.get)
    }
  }

  def checkDFResult(starDF: DataFrame,
                    expectedDF: DataFrame): Unit = {
    val starData = starDF.rdd.persist()
    val expectedData = expectedDF.rdd.persist()

    val firstDiff = expectedData.subtract(starData).persist()
    val secondDiff = starData.subtract(expectedData).persist()

    assert(starData.count() == expectedData.count())
    assert(firstDiff.count() == 0)
    assert(secondDiff.count() == 0)
  }


  def checkUpsertResult(tableName: String,
                        df: DataFrame,
                        expectedResults: DataFrame,
                        colNames: Seq[String],
                        condition: Option[String]): Unit = {

    executeUpsert(tableName, df, condition)

    val starData = StarTable.forPath(tableName).toDF
      .select(colNames.map(col): _*)

    checkDFResult(starData, expectedResults)
  }


}


class MergeOpInt extends MergeOperator[Int]{
  override def mergeData(input: Seq[Int]): Int = {
    input.sum
  }
}


class MergeOpString extends MergeOperator[String]{
  override def mergeData(input: Seq[String]): String = {
    input.mkString(",")
  }
}

class MergeOpString02 extends MergeOperator[String]{
  override def mergeData(input: Seq[String]): String = {
    input.mkString(";")
  }
}