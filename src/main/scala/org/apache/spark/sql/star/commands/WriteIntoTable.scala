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

import org.apache.spark.sql._
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.star.exception.StarLakeErrors
import org.apache.spark.sql.star.schema.ImplicitMetadataOperation
import org.apache.spark.sql.star.utils.DataFileInfo
import org.apache.spark.sql.star.{PartitionFilter, SnapshotManagement, StarLakeOptions, TransactionCommit}

/**
  * Used to write a [[DataFrame]] into a star table.
  *
  * New Table Semantics
  *  - The schema of the [[DataFrame]] is used to initialize the table.
  *  - The partition columns will be used to partition the table.
  *
  * Existing Table Semantics
  *  - The save mode will control how existing data is handled (i.e. overwrite, append, etc)
  *  - The schema will of the DataFrame will be checked and if there are new columns present
  * they will be added to the tables schema. Conflicting columns (i.e. a INT, and a STRING)
  * will result in an exception
  *  - The partition columns, if present are validated against the existing metadata. If not
  * present, then the partitioning of the table is respected.
  *
  * In combination with `Overwrite`, a `replaceWhere` option can be used to transactionally
  * replace data that matches a predicate.
  */
case class WriteIntoTable(snapshotManagement: SnapshotManagement,
                          mode: SaveMode,
                          options: StarLakeOptions,
                          configuration: Map[String, String],
                          data: DataFrame)
  extends RunnableCommand
    with ImplicitMetadataOperation
    with Command {

  override protected val canMergeSchema: Boolean = options.canMergeSchema

  private def isOverwriteOperation: Boolean = mode == SaveMode.Overwrite

  override protected val canOverwriteSchema: Boolean =
    options.canOverwriteSchema && isOverwriteOperation && options.replaceWhere.isEmpty

  override protected val rangePartitions: String = options.rangePartitions

  override protected val hashPartitions: String = options.hashPartitions

  override protected val hashBucketNum: Int = options.hashBucketNum

  override protected val shortTableName: Option[String] = options.shortTableName

  override protected val createMaterialView: Boolean = options.createMaterialView
  override protected val materialSQLText: String = options.materialSQLText
  override protected val materialAutoUpdate: Boolean = options.materialAutoUpdate


  override def run(sparkSession: SparkSession): Seq[Row] = {
    snapshotManagement.withNewTransaction { tc =>
      val (addFiles, expireFiles) = write(tc, sparkSession)
      tc.commit(addFiles, expireFiles)
    }
    Seq.empty
  }

  /** @return (newFiles, deletedFiles) */
  def write(tc: TransactionCommit, sparkSession: SparkSession): (Seq[DataFileInfo], Seq[DataFileInfo]) = {
    import sparkSession.implicits._

    val hashCols = if (tc.isFirstCommit) {
      hashPartitions
    } else {
      tc.tableInfo.hash_column
    }

    if (!tc.isFirstCommit) {
      // This table already exists, check if the insert is valid.
      if (mode == SaveMode.ErrorIfExists) {
        throw StarLakeErrors.pathAlreadyExistsException(snapshotManagement.snapshot.getTableInfo.table_path)
      }
      else if (mode == SaveMode.Append && hashCols.nonEmpty) {
        throw StarLakeErrors.appendNotSupportException
      } else if (mode == SaveMode.Ignore) {
        return (Nil, Nil)
      } else if (mode == SaveMode.Overwrite) {
        snapshotManagement.assertRemovable()
      }
    }
    updateMetadata(tc, data, configuration, isOverwriteOperation)

    // Validate partition predicates
    val replaceWhere = options.replaceWhere
    val partitionFilters = if (replaceWhere.isDefined) {
      val predicates = parsePartitionPredicates(sparkSession, replaceWhere.get)
      if (mode == SaveMode.Overwrite) {
        verifyPartitionPredicates(
          sparkSession, tc.tableInfo.range_column, predicates)
      }
      Some(predicates)
    } else {
      None
    }


    val newFiles = tc.writeFiles(data, Some(options))
    val deletedFiles = (mode, partitionFilters) match {
      case (SaveMode.Overwrite, None) =>
        val deleteTime = System.currentTimeMillis()
        tc.filterFiles().map(_.expire(deleteTime))
      case (SaveMode.Overwrite, Some(predicates)) =>
        // Check to make sure the files we wrote out were actually valid.
        val matchingFiles = PartitionFilter.filterFileList(
          tc.tableInfo.range_partition_schema, newFiles.toDF(), predicates).as[DataFileInfo].collect()
        val invalidFiles = newFiles.toSet -- matchingFiles
        if (invalidFiles.nonEmpty) {
          val badPartitions = invalidFiles
            .map(_.range_partitions)
            .map {
              _.map { case (k, v) => s"$k=$v" }.mkString("/")
            }
            .mkString(", ")
          throw StarLakeErrors.replaceWhereMismatchException(replaceWhere.get, badPartitions)
        }
        val deleteTime = System.currentTimeMillis()
        tc.filterFiles(predicates).map(_.expire(deleteTime))
      case _ => Nil
    }

    (newFiles, deletedFiles)
  }
}