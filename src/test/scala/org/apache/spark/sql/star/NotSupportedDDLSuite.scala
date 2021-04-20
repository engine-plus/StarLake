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

package org.apache.spark.sql.star

import java.util.Locale

import com.engineplus.star.tables.StarTable
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.star.test.StarLakeSQLCommandTest
import org.apache.spark.sql.test.{SQLTestUtils, SharedSparkSession}
import org.apache.spark.sql.{AnalysisException, QueryTest}

import scala.util.control.NonFatal


class NotSupportedDDLSuite
  extends NotSupportedDDLBase
    with SharedSparkSession
    with StarLakeSQLCommandTest


abstract class NotSupportedDDLBase extends QueryTest
  with SQLTestUtils {

  val format = "star"

  val nonPartitionedTableName = "starTbl"

  val partitionedTableName = "partitionedStarLakeTbl"

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    try {
      sql(
        s"""
           |CREATE TABLE $nonPartitionedTableName
           |USING $format
           |AS SELECT 1 as a, 'a' as b
         """.stripMargin)

      sql(
        s"""
           |CREATE TABLE $partitionedTableName (a INT, b STRING, p1 INT)
           |USING $format
           |PARTITIONED BY (p1)
          """.stripMargin)
      sql(s"INSERT INTO $partitionedTableName SELECT 1, 'A', 2")
    } catch {
      case NonFatal(e) =>
        afterAll()
        throw e
    }
  }

  protected override def afterAll(): Unit = {
    try {
      val location = Seq(nonPartitionedTableName, partitionedTableName).map(tbl => {
        try {
          Option(spark.sessionState.catalog.getTableMetadata(TableIdentifier(tbl)).location)
        } catch {
          case e: Exception => None
        }
      })

      sql(s"DROP TABLE IF EXISTS $nonPartitionedTableName")
      sql(s"DROP TABLE IF EXISTS $partitionedTableName")
      location.foreach(loc => {
        if (loc.isDefined) {
          try {
            StarTable.forPath(loc.get.toString).dropTable()
          } catch {
            case e: Exception =>
          }
        }
      })

    } finally {
      super.afterAll()
    }
  }

  private def assertUnsupported(query: String): Unit = {
    val e = intercept[AnalysisException] {
      sql(query)
    }
    assert(e.getMessage.toLowerCase(Locale.ROOT).contains("operation not allowed"))
  }

  private def assertIgnored(query: String): Unit = {
    val outputStream = new java.io.ByteArrayOutputStream()
    Console.withOut(outputStream) {
      sql(query)
    }
    assert(outputStream.toString.contains("The request is ignored"))
  }

  test("bucketing is not supported for star tables") {
    withTable("tbl") {
      assertUnsupported(
        s"""
           |CREATE TABLE tbl(a INT, b INT)
           |USING $format
           |CLUSTERED BY (a) INTO 5 BUCKETS
        """.stripMargin)
    }
  }

  test("CREATE TABLE LIKE") {
    withTable("tbl") {
      assertUnsupported(s"CREATE TABLE tbl LIKE $nonPartitionedTableName")
    }
  }

  test("ANALYZE TABLE PARTITION") {
    assertUnsupported(s"ANALYZE TABLE $partitionedTableName PARTITION (p1) COMPUTE STATISTICS")
  }

  test("ALTER TABLE ADD PARTITION") {
    assertUnsupported(s"ALTER TABLE $partitionedTableName ADD PARTITION (p1=3)")
  }

  test("ALTER TABLE DROP PARTITION") {
    assertUnsupported(s"ALTER TABLE $partitionedTableName DROP PARTITION (p1=2)")
  }

  test("ALTER TABLE RECOVER PARTITIONS") {
    assertUnsupported(s"ALTER TABLE $partitionedTableName RECOVER PARTITIONS")
    assertUnsupported(s"MSCK REPAIR TABLE $partitionedTableName")
  }

  test("ALTER TABLE SET SERDEPROPERTIES") {
    assertUnsupported(s"ALTER TABLE $nonPartitionedTableName SET SERDEPROPERTIES (s1=3)")
  }

  test("ALTER TABLE RENAME TO") {
    assertUnsupported(s"ALTER TABLE $nonPartitionedTableName RENAME TO newTbl")
  }


  test("LOAD DATA") {
    assertUnsupported(
      s"""LOAD DATA LOCAL INPATH '/path/to/home' INTO TABLE $nonPartitionedTableName""")
  }

  test("INSERT OVERWRITE DIRECTORY") {
    assertUnsupported(s"INSERT OVERWRITE DIRECTORY '/path/to/home' USING $format VALUES (1, 'a')")
  }
}
