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

import org.apache.spark.sql.catalyst.expressions.And
import org.apache.spark.sql.star._
import org.apache.spark.sql.star.exception.StarLakeErrors
import org.apache.spark.sql.star.utils.DataFileInfo
//import org.apache.spark.sql.star.actions.AddFile
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, Expression, Literal, NamedExpression, PredicateHelper}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.functions._
import org.apache.spark.sql.star.schema.ImplicitMetadataOperation
import org.apache.spark.sql.star.sources.StarLakeSQLConf
import org.apache.spark.sql.star.utils.AnalysisHelper
import org.apache.spark.sql.types.StructType

case class UpsertDataRows(rows: Long)

case class UpsertDataFiles(files: Long)

/** State for a Upsert operation */
case class UpsertStats(
                        // Expressions used in Upsert
                        conditionExpr: String,
                        updateConditionExpr: String,
                        updateExprs: Array[String],
                        insertConditionExpr: String,
                        insertExprs: Array[String],
                        deleteConditionExpr: String,

                        // Data sizes of source and target at different stages of processing
                        source: UpsertDataRows,
                        targetBeforeSkipping: UpsertDataFiles,
                        targetAfterSkipping: UpsertDataFiles,

                        // Data change sizes
                        targetFilesRemoved: Long,
                        targetFilesAdded: Long,
                        targetRowsCopied: Long,
                        targetRowsUpdated: Long,
                        targetRowsInserted: Long,
                        targetRowsDeleted: Long)

/**
  * Performs a Upsert of a source query/table into a Star table.
  *
  * Issues an error message when the ON search_condition of the MERGE statement can match
  * a single row from the target table with multiple rows of the source table-reference.
  *
  * Algorithm:
  *
  * Phase 1: Find the input files in target that are touched by the rows that satisfy
  * the condition and verify that no two source rows match with the same target row.
  * This is implemented as an inner-join using the given condition. See findTouchedFiles
  * for more details.
  *
  * Phase 2: Read the touched files again and write new files with updated and/or inserted rows.
  *
  * Phase 3: Atomically remove the touched files and add the new files.
  *
  * @param source                   Source data to merge from
  * @param target                   Target table to merge into
  * @param targetSnapshotManagement snapshotManagement of the target table
  * @param condition                Condition for a source row to match with a target row
  * @param migratedSchema           The final schema of the target - may be changed by schema evolution.
  */
case class UpsertCommand(@transient source: LogicalPlan,
                         @transient target: LogicalPlan,
                         @transient targetSnapshotManagement: SnapshotManagement,
                         condition: Expression,
                         migratedSchema: Option[StructType]) extends RunnableCommand
  with Command with PredicateHelper with AnalysisHelper with ImplicitMetadataOperation {

  private val tableInfo = targetSnapshotManagement.snapshot.getTableInfo

  override val canMergeSchema: Boolean = conf.getConf(StarLakeSQLConf.SCHEMA_AUTO_MIGRATE)
  override val canOverwriteSchema: Boolean = false
  override val rangePartitions: String = tableInfo.range_column
  override val hashPartitions: String = tableInfo.hash_column
  override val hashBucketNum: Int = tableInfo.bucket_num


  override def run(spark: SparkSession): Seq[Row] = {
    targetSnapshotManagement.withNewTransaction { tc =>
      if (target.schema.size != tableInfo.schema.size) {
        throw StarLakeErrors.schemaChangedSinceAnalysis(
          atAnalysis = target.schema, latestSchema = tableInfo.schema)
      }

      if (tableInfo.hash_column.isEmpty) {
        throw StarLakeErrors.hashColumnsIsNullException()
      }

      val canUseDeltaFile = spark.conf.get(StarLakeSQLConf.USE_DELTA_FILE)

      val sourceCols = source.output.map(_.name.stripPrefix("`").stripSuffix("`"))

      //source schema should have all the partition cols
      if (!tableInfo.partition_cols.forall(sourceCols.contains)) {
        throw StarLakeErrors
          .partitionColumnNotFoundException(
            tableInfo.partition_cols.mkString(","),
            sourceCols.mkString(","))
      }

      if (canMergeSchema) {
        updateMetadata(
          spark,
          tc,
          migratedSchema.getOrElse(target.schema),
          tc.tableInfo.configuration,
          isOverwriteMode = false)
      } else {
        val externalColumns = sourceCols.filterNot(tableInfo.schema.fieldNames.contains)
        if (externalColumns.nonEmpty) {
          throw StarLakeErrors.columnsNotFoundException(externalColumns)
        }
      }


      /** If delta file can be used, just write new data and delete nothing.
        * Else a merge data should be built and overwrite all files. */
      if (canUseDeltaFile) {
        tc.setCommitType("delta")

        val newFiles = tc.writeFiles(Dataset.ofRows(spark, source))
        tc.commit(newFiles, Seq.empty[DataFileInfo])
      } else {

        val targetOnlyPredicates = splitConjunctivePredicates(condition)
          .filter(f =>
            f.references.nonEmpty
              && f.references.forall(r => tableInfo.range_partition_columns.contains(r.name)))

        //condition should be declared for partitioned table by default
        if (tableInfo.range_column.nonEmpty
          && targetOnlyPredicates.isEmpty
          && !conf.getConf(StarLakeSQLConf.ALLOW_FULL_TABLE_UPSERT)) {
          throw StarLakeErrors.upsertConditionNotFoundException()
        }

        val dataSkippedFiles = tc.filterFiles(targetOnlyPredicates)

        val targetExistCols = dataSkippedFiles.flatMap(_.file_exist_cols.split(",")).distinct
        val needColumns = tableInfo.schema.fieldNames
        val repeatCols = sourceCols.intersect(targetExistCols)
        val allCols = sourceCols.union(targetExistCols).distinct


        val columnFilter = new Column(targetOnlyPredicates.reduceLeftOption(And).getOrElse(Literal(true)))
        val sourceDF = Dataset.ofRows(spark, source).filter(columnFilter)

        val targetDF = Dataset.ofRows(spark, buildTargetPlanWithFiles(tc, dataSkippedFiles, needColumns))

        var resultDF = targetDF.join(sourceDF, tableInfo.partition_cols, "full")

        if (repeatCols.nonEmpty) {
          resultDF = resultDF.select(allCols.map(column => {
            if (repeatCols.contains(column) && !tableInfo.partition_cols.contains(column)) {
              coalesce(sourceDF(column), targetDF(column)).as(column)
            } else {
              col(column)
            }
          }): _*)
        }

        val newFiles = tc.writeFiles(resultDF)
        tc.commit(newFiles, dataSkippedFiles)
      }
    }
    spark.sharedState.cacheManager.recacheByPlan(spark, target)
    Seq.empty
  }


  /**
    * Build a new logical plan using the given `files` that has the same output columns (exprIds)
    * as the `target` logical plan, so that existing update/insert expressions can be applied
    * on this new plan.
    */
  private def buildTargetPlanWithFiles(tc: TransactionCommit,
                                       files: Seq[DataFileInfo],
                                       selectCols: Seq[String]): LogicalPlan = {
    val plan = tc.snapshotManagement
      .createDataFrame(files, selectCols)
      .queryExecution.analyzed

    // For each plan output column, find the corresponding target output column (by name) and
    // create an alias
    val aliases = plan.output.map {
      case newAttrib: AttributeReference =>
        val existingTargetAttrib = getTargetOutputCols.find(_.name == newAttrib.name)
          .getOrElse {
            throw new AnalysisException(
              s"Could not find ${newAttrib.name} among the existing target output " +
                s"$getTargetOutputCols")
          }.asInstanceOf[AttributeReference]
        Alias(newAttrib, existingTargetAttrib.name)(exprId = existingTargetAttrib.exprId)
    }
    Project(aliases, plan)
  }

  private def getTargetOutputCols: Seq[NamedExpression] = {
    tableInfo.schema.map { col =>
      target.output.find(attr => conf.resolver(attr.name, col.name)).getOrElse {
        Alias(Literal(null, col.dataType), col.name)()
      }
    }
  }

}

