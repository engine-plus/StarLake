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

import java.net.URI

import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.spark.sql.catalyst.expressions.{Cast, Expression, GenericInternalRow, Literal}
import org.apache.spark.sql.execution.datasources.{FileIndex, PartitionDirectory, PartitionSpec, PartitioningAwareFileIndex}
import org.apache.spark.sql.star.StarLakeFileIndexUtils._
import org.apache.spark.sql.star.utils.DataFileInfo
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{AnalysisException, SparkSession}

import scala.collection.mutable

/** file index for data source v1 */
abstract class StarLakeFileIndex(spark: SparkSession,
                                 snapshotManagement: SnapshotManagement) extends FileIndex {

  override def rootPaths: Seq[Path] = snapshotManagement.snapshot.getTableInfo.table_path :: Nil

  override def refresh(): Unit = {}

  /**
    * Returns all matching/valid files by the given `partitionFilters` and `dataFilters`
    */
  def matchingFiles(partitionFilters: Seq[Expression],
                    dataFilters: Seq[Expression]): Seq[DataFileInfo]

  override def partitionSchema: StructType = snapshotManagement.snapshot.getTableInfo.range_partition_schema

  protected def absolutePath(child: String): Path = {
    val p = new Path(new URI(child))
    if (p.isAbsolute) {
      p
    } else {
      new Path(snapshotManagement.table_name, p)
    }
  }

  override def listFiles(partitionFilters: Seq[Expression],
                         dataFilters: Seq[Expression]): Seq[PartitionDirectory] = {
    val timeZone = spark.sessionState.conf.sessionLocalTimeZone
    matchingFiles(partitionFilters, dataFilters).groupBy(_.range_partitions).map {
      case (partitionValues, files) =>
        //所有的分区组合
        val rowValues: Array[Any] = partitionSchema.map { p =>
          Cast(Literal(partitionValues(p.name)), p.dataType, Option(timeZone)).eval()
        }.toArray

        //file status
        val fileStats = files.map { f =>
          new FileStatus(
            /* length */ f.size,
            /* isDir */ false,
            /* blockReplication */ 0,
            /* blockSize */ 1,
            /* modificationTime */ f.modification_time,
            absolutePath(f.file_path))
        }.toArray

        PartitionDirectory(new GenericInternalRow(rowValues), fileStats)
    }.toSeq
  }

}

case class DataFileIndex(spark: SparkSession,
                         snapshotManagement: SnapshotManagement,
                         partitionFilters: Seq[Expression] = Nil)
  extends StarLakeFileIndex(spark, snapshotManagement) {

  override def matchingFiles(partitionFilters: Seq[Expression],
                             dataFilters: Seq[Expression]): Seq[DataFileInfo] = {
    PartitionFilter.filesForScan(snapshotManagement.snapshot, this.partitionFilters ++ partitionFilters ++ dataFilters)
  }

  override def inputFiles: Array[String] = {
    PartitionFilter.filesForScan(snapshotManagement.snapshot, partitionFilters).map(f => absolutePath(f.file_path).toString)
  }

  override def sizeInBytes: Long = snapshotManagement.snapshot.sizeInBytes
}


/**
  * A [[StarLakeFileIndex]] that generates the list of files from a given list of files.
  */
class BatchDataFileIndex(spark: SparkSession,
                         snapshotManagement: SnapshotManagement,
                         files: Seq[DataFileInfo])
  extends StarLakeFileIndex(spark, snapshotManagement) {


  override def matchingFiles(partitionFilters: Seq[Expression],
                             dataFilters: Seq[Expression]): Seq[DataFileInfo] = {
    import spark.implicits._
    PartitionFilter.filterFileList(
      snapshotManagement.snapshot.getTableInfo.range_partition_schema,
      files.toDF(),
      partitionFilters)
      .as[DataFileInfo]
      .collect()
  }


  override def inputFiles: Array[String] = {
    files.map(file => absolutePath(file.file_path).toString).toArray
  }

  //  override def partitionSchema: StructType = snapshot.getRangePartitionColumns

  override val sizeInBytes: Long = files.map(_.size).sum

}


///**
//  * used in merge parquet files
//  */
//class MergeScanFileIndex(override val spark: SparkSession,
//                         override val snapshotManagement: SnapshotManagement,
//                         parameters: Map[String, String],
//                         userSpecifiedSchema: Option[StructType],
//                         files: Seq[DataFileInfo],
//                         allowFilter: Boolean = true)
//  extends StarLakeFileIndexV2(spark, snapshotManagement) {
//
//  //  lazy val tableName: String = snapshotManagement.snapshot.getTableInfo.table_name
//
//  override def getFileInfo(filters: Seq[Expression]): Seq[DataFileInfo] = files
//
//  override def rootPaths: Seq[Path] = snapshotManagement.snapshot.getTableInfo.table_path :: Nil
//
//
//  override def partitionSchema: StructType = snapshotManagement.snapshot.getTableInfo.range_partition_schema
//
//  override def allFiles(): Seq[FileStatus] = files.map { f =>
//    new FileStatus(
//      /* length */ f.size,
//      /* isDir */ false,
//      /* blockReplication */ 0,
//      /* blockSize */ 1,
//      /* modificationTime */ f.modification_time,
//      absolutePath(f.file_path, tableName))
//  }
//
//  override def listFiles(partitionFilters: Seq[Expression],
//                         dataFilters: Seq[Expression]): Seq[PartitionDirectory] = {
//    val timeZone = spark.sessionState.conf.sessionLocalTimeZone
//    val realFiles = if (allowFilter) {
//      matchingFiles(partitionFilters, dataFilters)
//    } else {
//      files
//    }
//
//    realFiles
//      .groupBy(_.range_partitions).map {
//      case (partitionValues, files) =>
//        val rowValues: Array[Any] = partitionSchema.map { p =>
//          Cast(Literal(partitionValues(p.name)), p.dataType, Option(timeZone)).eval()
//        }.toArray
//
//        //file status
//        val fileStats = files.map { f =>
//          new FileStatus(
//            /* length */ f.size,
//            /* isDir */ false,
//            /* blockReplication */ 0,
//            /* blockSize */ 1,
//            /* modificationTime */ f.modification_time,
//            absolutePath(f.file_path, tableName))
//        }.toArray
//
//        PartitionDirectory(new GenericInternalRow(rowValues), fileStats)
//    }.toSeq
//  }
//
//  def matchingFiles(partitionFilters: Seq[Expression],
//                    dataFilters: Seq[Expression] = Nil): Seq[DataFileInfo] = {
//    import spark.implicits._
//    PartitionFilter.filterFileList(
//      snapshotManagement.snapshot.getTableInfo.range_partition_schema,
//      files.toDF(),
//      partitionFilters)
//      .as[DataFileInfo]
//      .collect()
//  }
//
//
//  override def inputFiles: Array[String] = {
//    files.map(file => file.file_path).toArray
//  }
//
//
//  override val sizeInBytes: Long = files.map(_.size).sum
//
//
//  override def partitionSpec(): PartitionSpec = {
//    throw new AnalysisException(
//      s"Function partitionSpec() is not support in merge.")
//  }
//
//
//  override def leafFiles: mutable.LinkedHashMap[Path, FileStatus] = {
//    throw new AnalysisException(
//      s"Function leafFiles() is not support in merge.")
//  }
//
//  override def leafDirToChildrenFiles: Map[Path, Array[FileStatus]] = {
//    throw new AnalysisException(
//      s"Function leafDirToChildrenFiles() is not support in merge.")
//  }
//
//}

/** file index for data source v2 */
abstract class StarLakeFileIndexV2(val spark: SparkSession,
                                   val snapshotManagement: SnapshotManagement)
  extends PartitioningAwareFileIndex(spark, Map.empty[String, String], None) {

  lazy val tableName: String = snapshotManagement.table_name

  def getFileInfo(filters: Seq[Expression]): Seq[DataFileInfo] = matchingFiles(filters)


  override def rootPaths: Seq[Path] = snapshotManagement.snapshot.getTableInfo.table_path :: Nil

  override def refresh(): Unit = {}

  /**
    * Returns all matching/valid files by the given `partitionFilters` and `dataFilters`
    */
  def matchingFiles(partitionFilters: Seq[Expression],
                    dataFilters: Seq[Expression] = Nil): Seq[DataFileInfo]


  override def partitionSchema: StructType = snapshotManagement.snapshot.getTableInfo.range_partition_schema

  override def listFiles(partitionFilters: Seq[Expression],
                         dataFilters: Seq[Expression]): Seq[PartitionDirectory] = {
    val timeZone = spark.sessionState.conf.sessionLocalTimeZone

    matchingFiles(partitionFilters, dataFilters)
      .groupBy(_.range_partitions).map {
      case (partitionValues, files) =>
        val rowValues: Array[Any] = partitionSchema.map { p =>
          Cast(Literal(partitionValues(p.name)), p.dataType, Option(timeZone)).eval()
        }.toArray

        //file status
        val fileStats = files.map { f =>
          new FileStatus(
            /* length */ f.size,
            /* isDir */ false,
            /* blockReplication */ 0,
            /* blockSize */ 1,
            /* modificationTime */ f.modification_time,
            absolutePath(f.file_path, tableName))
        }.toArray

        PartitionDirectory(new GenericInternalRow(rowValues), fileStats)
    }.toSeq
  }


  override def partitionSpec(): PartitionSpec = {
    throw new AnalysisException(
      s"Function partitionSpec() is not support in merge.")
  }


  override def leafFiles: mutable.LinkedHashMap[Path, FileStatus] = {
    throw new AnalysisException(
      s"Function leafFiles() is not support in merge.")
  }

  override def leafDirToChildrenFiles: Map[Path, Array[FileStatus]] = {
    throw new AnalysisException(
      s"Function leafDirToChildrenFiles() is not support in merge.")
  }

}


case class DataFileIndexV2(override val spark: SparkSession,
                           override val snapshotManagement: SnapshotManagement,
                           partitionFilters: Seq[Expression] = Nil)
  extends StarLakeFileIndexV2(spark, snapshotManagement) {

  override def matchingFiles(partitionFilters: Seq[Expression],
                             dataFilters: Seq[Expression]): Seq[DataFileInfo] = {
    PartitionFilter.filesForScan(
      snapshotManagement.snapshot,
      this.partitionFilters ++ partitionFilters ++ dataFilters)
  }

  override def inputFiles: Array[String] = {
    PartitionFilter.filesForScan(snapshotManagement.snapshot, partitionFilters)
      .map(f => absolutePath(f.file_path, tableName).toString)
  }

  override def sizeInBytes: Long = snapshotManagement.snapshot.sizeInBytes
}


/**
  * A [[StarLakeFileIndexV2]] that generates the list of files from a given list of files
  * that are within a version range of SnapshotManagement.
  */
case class BatchDataFileIndexV2(override val spark: SparkSession,
                                override val snapshotManagement: SnapshotManagement,
                                files: Seq[DataFileInfo])
  extends StarLakeFileIndexV2(spark, snapshotManagement) {


  override def matchingFiles(partitionFilters: Seq[Expression],
                             dataFilters: Seq[Expression]): Seq[DataFileInfo] = {
    import spark.implicits._
    PartitionFilter.filterFileList(
      snapshotManagement.snapshot.getTableInfo.range_partition_schema,
      files.toDF(),
      partitionFilters)
      .as[DataFileInfo]
      .collect()
  }


  override def inputFiles: Array[String] = {
    files.map(file => absolutePath(file.file_path, tableName).toString).toArray
  }


  override val sizeInBytes: Long = files.map(_.size).sum

}


object StarLakeFileIndexUtils {
  def absolutePath(child: String, tableName: String): Path = {
    val p = new Path(new URI(child))
    if (p.isAbsolute) {
      p
    } else {
      new Path(tableName, p)
    }
  }


}

