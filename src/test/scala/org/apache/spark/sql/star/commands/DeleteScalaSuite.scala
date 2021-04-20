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

import com.engineplus.star.tables
import com.engineplus.star.tables.{StarTableTestUtils, StarTable}
import org.apache.spark.sql.star.SnapshotManagement
import org.apache.spark.sql.star.test.StarLakeSQLCommandTest
import org.apache.spark.sql.{Row, functions}

class DeleteScalaSuite extends DeleteSuiteBase with StarLakeSQLCommandTest {

  import testImplicits._


  test("delete cached table by path") {
    Seq((2, 2), (1, 4)).toDF("key", "value")
      .write.mode("overwrite").format("star").save(tempPath)
    spark.read.format("star").load(tempPath).cache()
    spark.read.format("star").load(tempPath).collect()
    executeDelete(s"star.`$tempPath`", where = "key = 2")
    checkAnswer(spark.read.format("star").load(tempPath), Row(1, 4) :: Nil)
  }

  test("delete usage test - without condition") {
    append(Seq((1, 10), (2, 20), (3, 30), (4, 40)).toDF("key", "value"))
    val table = StarTable.forPath(tempPath)
    table.delete()
    checkAnswer(readStarLakeTable(tempPath), Nil)
  }

  test("delete usage test - with condition") {
    append(Seq((1, 10), (2, 20), (3, 30), (4, 40)).toDF("key", "value"))
    val table = tables.StarTable.forPath(tempPath)
    table.delete("key = 1 or key = 2")
    checkAnswer(readStarLakeTable(tempPath), Row(3, 30) :: Row(4, 40) :: Nil)
  }

  test("delete usage test - with Column condition") {
    append(Seq((1, 10), (2, 20), (3, 30), (4, 40)).toDF("key", "value"))
    val table = tables.StarTable.forPath(tempPath)
    table.delete(functions.expr("key = 1 or key = 2"))
    checkAnswer(readStarLakeTable(tempPath), Row(3, 30) :: Row(4, 40) :: Nil)
  }

  override protected def executeDelete(target: String, where: String = null): Unit = {

    def parse(tableNameWithAlias: String): (String, Option[String]) = {
      tableNameWithAlias.split(" ").toList match {
        case tableName :: Nil => tableName -> None // just table name
        case tableName :: alias :: Nil => // tablename SPACE alias OR tab SPACE lename
          val ordinary = (('a' to 'z') ++ ('A' to 'Z') ++ ('0' to '9')).toSet
          if (!alias.forall(ordinary.contains(_))) {
            (tableName + " " + alias) -> None
          } else {
            tableName -> Some(alias)
          }
        case _ =>
          fail(s"Could not build parse '$tableNameWithAlias' for table and optional alias")
      }
    }

    val starTable: StarTable = {
      val (tableNameOrPath, optionalAlias) = parse(target)
      val isPath: Boolean = tableNameOrPath.startsWith("star.")
      val table = if (isPath) {
        val path = tableNameOrPath.stripPrefix("star.`").stripSuffix("`")
        tables.StarTable.forPath(spark, path)
      } else {
        StarTableTestUtils.createTable(spark.table(tableNameOrPath),
          SnapshotManagement(tableNameOrPath))
      }
      optionalAlias.map(table.as(_)).getOrElse(table)
    }

    if (where != null) {
      starTable.delete(where)
    } else {
      starTable.delete()
    }
  }
}
