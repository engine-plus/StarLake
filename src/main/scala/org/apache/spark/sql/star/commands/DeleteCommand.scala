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

import org.apache.spark.sql.catalyst.analysis.EliminateSubqueryAliases
import org.apache.spark.sql.catalyst.expressions.{EqualNullSafe, Expression, InputFileName, Literal, Not}
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, StarDelete}
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.star._
import org.apache.spark.sql.star.catalog.StarLakeTableV2
import org.apache.spark.sql.star.exception.StarLakeErrors
import org.apache.spark.sql.star.utils.DataFileInfo
import org.apache.spark.sql.types.BooleanType
import org.apache.spark.sql.{Column, Dataset, Row, SparkSession}

/**
  * Performs a Delete based on the search condition
  *
  * Algorithm:
  * 1) Scan all the files and determine which files have
  * the rows that need to be deleted.
  * 2) Traverse the affected files and rebuild the touched files.
  * 3) Atomically write the remaining rows to new files and remove
  * the affected files that are identified in step 1.
  */
case class DeleteCommand(snapshotManagement: SnapshotManagement,
                         target: LogicalPlan,
                         condition: Option[Expression])
  extends RunnableCommand with Command {

  override def innerChildren: Seq[QueryPlan[_]] = Seq(target)

  final override def run(sparkSession: SparkSession): Seq[Row] = {

    snapshotManagement.assertRemovable()
    snapshotManagement.withNewTransaction { tc =>
      performDelete(sparkSession, tc)
    }
    // Re-cache all cached plans(including this relation itself, if it's cached) that refer to
    // this data source relation.

    //    val transPlan = EliminateSubqueryAliases(target) match {
    //      case r @ DataSourceV2Relation(_: StarLakeTableV2,_,_, ident,_) =>
    //        r.copy(catalog = None,
    //          identifier = None,
    //          options = new CaseInsensitiveStringMap(Map("path" -> ident.get.name())))
    //    }

    sparkSession.sharedState.cacheManager.recacheByPlan(sparkSession, target)

    Seq.empty[Row]
  }

  private def performDelete(sparkSession: SparkSession,
                            tc: TransactionCommit): Unit = {
    import sparkSession.implicits._

    //deleteFiles(addFiles, expireFiles)
    val (addFiles, expireFiles): (Seq[DataFileInfo], Seq[DataFileInfo]) = condition match {
      case None =>
        // Case 1: Delete the whole table if the condition is true
        val allFiles = tc.filterFiles(Nil)

        val deleteTime = System.currentTimeMillis()
        //no add files, delete all exists files
        (Seq.empty[DataFileInfo], allFiles.map(_.expire(deleteTime)))
      case Some(cond) =>
        val (metadataPredicates, otherPredicates) =
          StarLakeUtils.splitMetadataAndDataPredicates(
            cond, tc.tableInfo.range_partition_columns, sparkSession)

        if (otherPredicates.isEmpty) {
          // Case 2: The condition can be evaluated using metadata only.
          //         Delete a set of files without the need of scanning any data files.
          val candidateFiles = tc.filterFiles(metadataPredicates)

          val deleteTime = System.currentTimeMillis()
          (Seq.empty[DataFileInfo], candidateFiles.map(_.expire(deleteTime)))
        } else {
          // Case 3: Delete the rows based on the condition.
          val candidateFiles = tc.filterFiles(metadataPredicates ++ otherPredicates)

          val nameToFileMap = generateCandidateFileMap(candidateFiles)

          // Keep everything from the resolved target except a new FileIndex
          // that only involves the affected files instead of all files.
          val newTarget = StarLakeUtils.replaceFileIndexV2(target, candidateFiles)
          val data = Dataset.ofRows(sparkSession, newTarget)

          val filesToRewrite =
            if (candidateFiles.isEmpty) {
              Array.empty[String]
              //input_file_name() can't get correct file name when using merge file reader
            } else if (tc.tableInfo.hash_partition_columns.isEmpty) {
              data
                .filter(new Column(cond))
                .select(new Column(InputFileName())).distinct()
                .as[String].collect()
            } else {
              candidateFiles.map(_.file_path).toArray
            }

          if (filesToRewrite.isEmpty) {
            // Case 3.1: no row matches and no delete will be triggered
            (Nil, Nil)
          } else {
            // Case 3.2: some files need an update to remove the deleted files
            // Do the second pass and just read the affected files
            val rewriteFileInfo = filesToRewrite.map(f => getTouchedFile(f, nameToFileMap))

            // Keep everything from the resolved target except a new FileIndex
            // that only involves the affected files instead of all files.
            val newTarget = StarLakeUtils.replaceFileIndexV2(target, rewriteFileInfo)

            val targetDF = Dataset.ofRows(sparkSession, newTarget)
            val filterCond = Not(EqualNullSafe(cond, Literal(true, BooleanType)))
            val updatedDF = targetDF.filter(new Column(filterCond))

            val rewrittenFiles = tc.writeFiles(updatedDF)

            val operationTimestamp = System.currentTimeMillis()
            val expireFiles = removeFilesFromPaths(nameToFileMap, filesToRewrite, operationTimestamp)

            (rewrittenFiles, expireFiles)
          }
        }
    }
    if (addFiles.nonEmpty || expireFiles.nonEmpty) {
      tc.commit(addFiles, expireFiles)
    }
  }
}

object DeleteCommand {
  def apply(delete: StarDelete): DeleteCommand = {
    val snapshotManagement = EliminateSubqueryAliases(delete.child) match {
      case StarLakeTableRelationV2(tbl: StarLakeTableV2) => tbl.snapshotManagement
      case o =>
        throw StarLakeErrors.notAnStarLakeSourceException("DELETE", Some(o))
    }
    DeleteCommand(snapshotManagement, delete.child, delete.condition)
  }

}


