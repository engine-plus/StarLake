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

package org.apache.spark.sql.star.rules

import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.star.StarLakeUtils
import org.apache.spark.sql.star.exception.StarLakeErrors
import org.apache.spark.sql.star.sources.StarLakeSourceUtils

/**
  * A rule to check whether the functions are supported only when Hive support is enabled
  */
case class StarLakeUnsupportedOperationsCheck(spark: SparkSession)
  extends (LogicalPlan => Unit)
    with Logging {

  private def fail(operation: String, tableIdent: TableIdentifier): Unit = {
    if (StarLakeUtils.isStarLakeTable(spark, tableIdent)) {
      throw StarLakeErrors.operationNotSupportedException(operation, tableIdent)
    }
  }

  private def fail(operation: String, provider: String): Unit = {
    if (StarLakeSourceUtils.isStarLakeDataSourceName(provider)) {
      throw StarLakeErrors.operationNotSupportedException(operation)
    }
  }

  def apply(plan: LogicalPlan): Unit = {
    plan.foreach {
      case c: CreateTableLikeCommand =>
        fail(operation = "CREATE TABLE LIKE", c.sourceTable)

      case a: AnalyzePartitionCommand =>
        fail(operation = "ANALYZE TABLE PARTITION", a.tableIdent)

      case a: AlterTableAddPartitionCommand =>
        fail(operation = "ALTER TABLE ADD PARTITION", a.tableName)

      case a: AlterTableDropPartitionCommand =>
        fail(operation = "ALTER TABLE DROP PARTITION", a.tableName)

      case a: AlterTableRecoverPartitionsCommand =>
        fail(operation = "ALTER TABLE RECOVER PARTITIONS", a.tableName)

      case a: AlterTableSerDePropertiesCommand =>
        fail(operation = "ALTER TABLE table SET SERDEPROPERTIES", a.tableName)

      case l: LoadDataCommand =>
        fail(operation = "LOAD DATA", l.table)

      case i: InsertIntoDataSourceDirCommand =>
        fail(operation = "INSERT OVERWRITE DIRECTORY", i.provider)

      case r: AlterTableRenameCommand =>
        fail(operation = "RENAME TO", r.oldName)

      case _ => // OK
    }
  }
}
