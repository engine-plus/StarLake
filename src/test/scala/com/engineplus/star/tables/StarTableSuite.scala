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

package com.engineplus.star.tables

import java.util.Locale

import org.apache.spark.sql.star.StarLakeUtils
import org.apache.spark.sql.star.test.StarLakeSQLCommandTest
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.{AnalysisException, QueryTest}

class StarTableSuite extends QueryTest
  with SharedSparkSession
  with StarLakeSQLCommandTest {

  test("forPath") {
    withTempDir { dir =>
      testData.write.format("star").save(dir.getAbsolutePath)
      checkAnswer(
        StarTable.forPath(spark, dir.getAbsolutePath).toDF,
        testData.collect().toSeq)
      checkAnswer(
        StarTable.forPath(dir.getAbsolutePath).toDF,
        testData.collect().toSeq)
    }
  }


  test("forName") {
    withTempDir { dir =>
      withTable("starTable") {
        testData.write.format("star").saveAsTable("starTable")

        checkAnswer(
          StarTable.forName(spark, "starTable").toDF,
          testData.collect().toSeq)
        checkAnswer(
          StarTable.forName("starTable").toDF,
          testData.collect().toSeq)

      }
    }
  }

  def testForNameOnNonStarLakeName(tableName: String): Unit = {
    val msg = "not an Star table"
    testError(msg) {
      StarTable.forName(spark, tableName)
    }
    testError(msg) {
      StarTable.forName(tableName)
    }
  }

  test("forName - with non-Star table name") {
    withTempDir { dir =>
      withTable("notAnStarLakeTable") {
        testData.write.format("parquet").mode("overwrite")
          .saveAsTable("notAStarLakeTable")
        testForNameOnNonStarLakeName("notAnStarLakeTable")
      }
    }
  }

  test("forName - with temp view name") {
    withTempDir { dir =>
      withTempView("viewOnStarLakeTable") {
        testData.write.format("star").save(dir.getAbsolutePath)
        spark.read.format("star").load(dir.getAbsolutePath)
          .createOrReplaceTempView("viewOnStarLakeTable")
        testForNameOnNonStarLakeName("viewOnStarLakeTable")
      }
    }
  }

  test("forName - with star.`path`") {
    withTempDir { dir =>
      testData.write.format("star").save(dir.getAbsolutePath)
      testForNameOnNonStarLakeName(s"star.`$dir`")
    }
  }

  test("as") {
    withTempDir { dir =>
      testData.write.format("star").save(dir.getAbsolutePath)
      checkAnswer(
        StarTable.forPath(dir.getAbsolutePath).as("tbl").toDF.select("tbl.value"),
        testData.select("value").collect().toSeq)
    }
  }

  test("isStarLakeTable - path") {
    withTempDir { dir =>
      testData.write.format("star").save(dir.getAbsolutePath)
      assert(StarLakeUtils.isStarLakeTable(dir.getAbsolutePath))
    }
  }

  test("isStarLakeTable - with non-Star table path") {
    withTempDir { dir =>
      testData.write.format("parquet").mode("overwrite").save(dir.getAbsolutePath)
      assert(!StarLakeUtils.isStarLakeTable(dir.getAbsolutePath))
    }
  }

  def testError(expectedMsg: String)(thunk: => Unit): Unit = {
    val e = intercept[AnalysisException] {
      thunk
    }
    assert(e.getMessage.toLowerCase(Locale.ROOT).contains(expectedMsg.toLowerCase(Locale.ROOT)))
  }


}
