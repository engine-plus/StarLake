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

package com.engineplus.star.tables.execution

import com.engineplus.star.meta.MetaVersion
import com.engineplus.star.tables.StarTable
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.{Assignment, DeleteFromTable, StarUpsert, UpdateTable}
import org.apache.spark.sql.star.SnapshotManagement
import org.apache.spark.sql.star.commands._
import org.apache.spark.sql.star.exception.StarLakeErrors
import org.apache.spark.sql.star.rules.PreprocessTableUpsert
import org.apache.spark.sql.star.sources.StarLakeSQLConf
import org.apache.spark.sql.star.utils.AnalysisHelper
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Column, DataFrame, SparkSession, functions}

import scala.collection.mutable

trait StarTableOperations extends AnalysisHelper {
  self: StarTable =>

  protected def sparkSession: SparkSession = self.toDF.sparkSession

  protected def executeDelete(condition: Option[Expression]): Unit = {
    val delete = DeleteFromTable(self.toDF.queryExecution.analyzed, condition)
    toDataset(sparkSession, delete)
  }


  protected def toStrColumnMap(map: Map[String, String]): Map[String, Column] = {
    map.toSeq.map { case (k, v) => k -> functions.expr(v) }.toMap
  }

  protected def executeUpdate(set: Map[String, Column], condition: Option[Column]): Unit = {
    val assignments = set.map { case (targetColName, column) =>
      Assignment(UnresolvedAttribute.quotedString(targetColName), column.expr)
    }.toSeq
    val update = UpdateTable(self.toDF.queryExecution.analyzed, assignments, condition.map(_.expr))
    toDataset(sparkSession, update)
  }


  protected def executeUpsert(targetTable: StarTable,
                              sourceDF: DataFrame,
                              condition: String): Unit = {

    val target = targetTable.toDF.queryExecution.analyzed
    val source = sourceDF.queryExecution.analyzed

    val shouldAutoMigrate = sparkSession.sessionState.conf.getConf(StarLakeSQLConf.SCHEMA_AUTO_MIGRATE)
    // Migrated schema to be used for schema evolution.
    val finalSchema = if (shouldAutoMigrate) {
      // We can't just use the merge method in StructType, because it doesn't account
      // for possible implicit conversions. Instead, we use the target schema for all
      // existing columns and the source schema only for new ones.
      val targetSchema = target.schema
      val migratedSchema = mutable.ListBuffer[StructField]()
      targetSchema.foreach(migratedSchema.append(_))

      source.schema.foreach { col =>
        val isInTarget = targetSchema.exists { targetCol =>
          target.conf.resolver(targetCol.name, col.name)
        }
        if (!isInTarget) {
          migratedSchema.append(col)
        }
      }

      StructType(migratedSchema)
    } else {
      target.schema
    }

    val upsert = StarUpsert(
      target,
      source,
      condition,
      if (shouldAutoMigrate) Some(finalSchema) else None)

    toDataset(sparkSession, PreprocessTableUpsert(sparkSession.sessionState.conf)(upsert))

  }


  protected def executeCompaction(df: DataFrame,
                                  snapshotManagement: SnapshotManagement,
                                  condition: String,
                                  force: Boolean = true,
                                  mergeOperatorInfo: Map[String, String]): Unit = {
    toDataset(sparkSession, CompactionCommand(
      snapshotManagement,
      condition,
      force,
      mergeOperatorInfo))

  }


  protected def executeCleanup(snapshotManagement: SnapshotManagement,
                               justList: Boolean): Unit = {
    CleanupCommand.runCleanup(sparkSession, snapshotManagement, justList)
  }

  protected def executeDropTable(snapshotManagement: SnapshotManagement): Unit = {
    val snapshot = snapshotManagement.snapshot
    val tableInfo = snapshot.getTableInfo
    if (!MetaVersion.isTableIdExists(tableInfo.table_name, tableInfo.table_id)) {
      StarLakeErrors.tableNotFoundException(tableInfo.table_name, tableInfo.table_id)
    }
    DropTableCommand.run(snapshot)
  }

  protected def executeDropPartition(snapshotManagement: SnapshotManagement,
                                     condition: Expression): Unit = {
    DropPartitionCommand.run(
      snapshotManagement.snapshot,
      condition)
  }


  protected def executeUpdateForMaterialView(snapshotManagement: SnapshotManagement): Unit = {
    toDataset(sparkSession, UpdateMaterialViewCommand(snapshotManagement))
  }

}
