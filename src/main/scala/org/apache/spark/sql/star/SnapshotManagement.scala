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

import java.io.File
import java.util.concurrent.locks.ReentrantLock

import com.engineplus.star.meta.{MetaUtils, MetaVersion}
import org.apache.hadoop.fs.Path
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.sources.BaseRelation
import org.apache.spark.sql.star.catalog.StarLakeTableV2
import org.apache.spark.sql.star.exception.StarLakeErrors
import org.apache.spark.sql.star.sources.{StarLakeBaseRelation, StarLakeSourceUtils}
import org.apache.spark.sql.star.utils.{DataFileInfo, PartitionInfo, TableInfo}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.{AnalysisException, DataFrame, Dataset, SparkSession}

import scala.collection.JavaConversions._

class SnapshotManagement(path: String) extends Logging {

  val table_name: String = MetaUtils.modifyTableString(path)

  lazy private val spark: SparkSession = SparkSession.active

  lazy private val lock = new ReentrantLock()

  private var currentSnapshot: Snapshot = getCurrentSnapshot

  def snapshot: Snapshot = currentSnapshot

  private def createSnapshot: Snapshot = {
    val table_info = MetaVersion.getTableInfo(table_name)
    val partition_info_arr = MetaVersion.getAllPartitionInfo(table_info.table_id)

    if (table_info.table_schema.isEmpty) {
      throw StarLakeErrors.schemaNotSetException
    }
    new Snapshot(table_info, partition_info_arr)
  }

  private def initSnapshot: Snapshot = {
    val table_path = new Path(table_name)
    val fs = table_path.getFileSystem(spark.sessionState.newHadoopConf())
    if (fs.exists(table_path) && fs.listStatus(table_path).nonEmpty) {
      throw StarLakeErrors.failedCreateTableException(table_name)
    }

    val table_id = "table_" + java.util.UUID.randomUUID().toString
    val range_id = "range_" + java.util.UUID.randomUUID().toString
    val table_info = TableInfo(table_name, table_id)
    val partition_arr = Array(
      PartitionInfo(table_id, range_id, table_name, MetaUtils.DEFAULT_RANGE_PARTITION_VALUE, 1, 1)
    )
    new Snapshot(table_info, partition_arr, true)
  }


  private def getCurrentSnapshot: Snapshot = {
    if (StarLakeSourceUtils.isStarLakeTableExists(table_name)) {
      createSnapshot
    } else {
      //table_name in SnapshotManagement must be a root path, and its parent path shouldn't be star table
      if (StarLakeUtils.isStarLakeTable(table_name)) {
        throw new AnalysisException("table_name is expected as root path in SnapshotManagement")
      }
      initSnapshot
    }
  }

  def updateSnapshot(): Snapshot = {
    lockInterruptibly {
      val new_snapshot = getCurrentSnapshot
      currentSnapshot = new_snapshot
      currentSnapshot
    }
  }

  //get table info only
  def getTableInfoOnly: TableInfo = {
    if (StarLakeSourceUtils.isStarLakeTableExists(table_name)) {
      MetaVersion.getTableInfo(table_name)
    } else {
      val table_id = "table_" + java.util.UUID.randomUUID().toString
      TableInfo(table_name, table_id)
    }
  }

  def startTransaction(): TransactionCommit = {
    updateSnapshot()
    new TransactionCommit(this)
  }

  /**
    * Execute a piece of code within a new [[TransactionCommit]]. Reads/write sets will
    * be recorded for this table, and all other tables will be read
    * at a snapshot that is pinned on the first access.
    *
    * @note This uses thread-local variable to make the active transaction visible. So do not use
    *       multi-threaded code in the provided thunk.
    */
  def withNewTransaction[T](thunk: TransactionCommit => T): T = {
    try {
      updateSnapshot()
      val tc = new TransactionCommit(this)
      TransactionCommit.setActive(tc)
      thunk(tc)
    } finally {
      TransactionCommit.clearActive()
    }
  }

  /**
    * Checks whether this table only accepts appends. If so it will throw an error in operations that
    * can remove data such as DELETE/UPDATE/MERGE.
    */
  def assertRemovable(): Unit = {
    if (StarLakeConfig.IS_APPEND_ONLY.fromTableInfo(snapshot.getTableInfo)) {
      throw StarLakeErrors.modifyAppendOnlyTableException
    }
  }

  def assertTableNameIsRootPath: Unit = {
  }

  def createRelation(partitionFilters: Seq[Expression] = Nil): BaseRelation = {
    val files: Array[DataFileInfo] = PartitionFilter.filesForScan(snapshot, partitionFilters)
    StarLakeBaseRelation(files, this)(spark)
  }


  def createDataFrame(files: Seq[DataFileInfo], requiredColumns: Seq[String], predicts: Option[Expression] = None): DataFrame = {
    val skipFiles = if (predicts.isDefined) {
      val predictFiles = PartitionFilter.filesForScan(snapshot, Seq(predicts.get))
      files.intersect(predictFiles)
    } else {
      files
    }

    if (snapshot.getTableInfo.hash_partition_columns.isEmpty) {
      val fileIndex = new BatchDataFileIndex(spark, this, files)

      val table_info = snapshot.getTableInfo

      val relation = HadoopFsRelation(
        fileIndex,
        partitionSchema = snapshot.getTableInfo.range_partition_schema,
        dataSchema = table_info.schema,
        bucketSpec = None,
        snapshot.fileFormat,
        snapshot.getTableInfo.format.options)(spark)

      Dataset.ofRows(spark, LogicalRelation(relation, isStreaming = false))
    } else {
      val fileIndex = BatchDataFileIndexV2(spark, this, skipFiles)
      val table = StarLakeTableV2(
        spark,
        new Path(table_name),
        None,
        None,
        Option(fileIndex)
      )
      val option = new CaseInsensitiveStringMap(Map("basePath" -> table_name))
      Dataset.ofRows(
        spark,
        DataSourceV2ScanRelation(
          table,
          table.newScanBuilder(option).build(),
          table.schema().toAttributes
        )
      ).select(requiredColumns.map(col): _*)
    }
  }

  def lockInterruptibly[T](body: => T): T = {
    lock.lockInterruptibly()
    try {
      body
    } finally {
      lock.unlock()
    }
  }

}

case class GroupFiles(key: (Map[String, String], Int), dataFiles: Seq[DataFileInfo])

object SnapshotManagement {

  def apply(path: String): SnapshotManagement = new SnapshotManagement(path)

  def apply(path: Path): SnapshotManagement = new SnapshotManagement(path.toString)

  def forTable(spark: SparkSession, tableName: TableIdentifier): SnapshotManagement = {
    val catalog = spark.sessionState.catalog
    val catalogTable = catalog.getTableMetadata(tableName).location
    apply(new Path(catalogTable))
  }

  def forTable(dataPath: File): SnapshotManagement = {
    apply(new Path(dataPath.getAbsolutePath))
  }

}
