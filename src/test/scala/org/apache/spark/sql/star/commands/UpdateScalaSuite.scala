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

import java.util.Locale

import com.engineplus.star.tables
import com.engineplus.star.tables.{StarTable, StarTableTestUtils}
import org.apache.spark.sql.star.SnapshotManagement
import org.apache.spark.sql.star.test.StarLakeSQLCommandTest
import org.apache.spark.sql.{Row, functions}

class UpdateScalaSuite extends UpdateSuiteBase with StarLakeSQLCommandTest {

  import testImplicits._

  test("update cached table") {
    Seq((2, 2), (1, 4)).toDF("key", "value")
      .write.mode("overwrite").format("star").save(tempPath)

    spark.read.format("star").load(tempPath).cache()
    spark.read.format("star").load(tempPath).collect()

    executeUpdate(s"star.`$tempPath`", set = "key = 3")
    checkAnswer(spark.read.format("star").load(tempPath), Row(3, 2) :: Row(3, 4) :: Nil)
  }


  test("update usage test - without condition") {
    append(Seq((1, 10), (2, 20), (3, 30), (4, 40)).toDF("key", "value"))
    val table = tables.StarTable.forPath(tempPath)
    table.updateExpr(Map("key" -> "100"))
    checkAnswer(readStarLakeTable(tempPath),
      Row(100, 10) :: Row(100, 20) :: Row(100, 30) :: Row(100, 40) :: Nil)
  }

  test("update usage test - without condition, using Column") {
    append(Seq((1, 10), (2, 20), (3, 30), (4, 40)).toDF("key", "value"))
    val table = tables.StarTable.forPath(tempPath)
    table.update(Map("key" -> functions.expr("100")))
    checkAnswer(readStarLakeTable(tempPath),
      Row(100, 10) :: Row(100, 20) :: Row(100, 30) :: Row(100, 40) :: Nil)
  }

  test("update usage test - with condition") {
    append(Seq((1, 10), (2, 20), (3, 30), (4, 40)).toDF("key", "value"))
    val table = tables.StarTable.forPath(tempPath)
    table.updateExpr("key = 1 or key = 2", Map("key" -> "100"))
    checkAnswer(readStarLakeTable(tempPath),
      Row(100, 10) :: Row(100, 20) :: Row(3, 30) :: Row(4, 40) :: Nil)
  }

  test("update usage test - with condition, using Column") {
    append(Seq((1, 10), (2, 20), (3, 30), (4, 40)).toDF("key", "value"))
    val table = tables.StarTable.forPath(tempPath)
    table.update(functions.expr("key = 1 or key = 2"),
      Map("key" -> functions.expr("100"), "value" -> functions.expr("101")))
    checkAnswer(readStarLakeTable(tempPath),
      Row(100, 101) :: Row(100, 101) :: Row(3, 30) :: Row(4, 40) :: Nil)
  }

  override protected def executeUpdate(target: String,
                                       set: String,
                                       where: String = null): Unit = {
    executeUpdate(target, set.split(","), where)
  }

  override protected def executeUpdate(target: String,
                                       set: Seq[String],
                                       where: String): Unit = {

    def parse(tableNameWithAlias: String): (String, Option[String]) = {
      tableNameWithAlias.split(" ").toList match {
        case tableName :: Nil => tableName -> None
        case tableName :: alias :: Nil =>
          val ordinary = (('a' to 'z') ++ ('A' to 'Z') ++ ('0' to '9')).toSet
          if (alias.forall(ordinary.contains(_))) {
            tableName -> Some(alias)
          } else {
            tableName + " " + alias -> None
          }
        case list if list.size >= 3 && list(list.size - 2).toLowerCase(Locale.ROOT) == "as" =>
          list.dropRight(2).mkString(" ").trim() -> Some(list.last)
        case list if list.size >= 2 =>
          list.dropRight(1).mkString(" ").trim() -> Some(list.last)
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

    val setColumns = set.map { assign =>
      val kv = assign.split("=")
      require(kv.size == 2)
      kv(0).trim -> kv(1).trim
    }.toMap

    if (where == null) {
      starTable.updateExpr(setColumns)
    } else {
      starTable.updateExpr(where, setColumns)
    }
  }
}
