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

package org.apache.spark.sql.execution.datasources.v2.merge

import org.apache.hadoop.fs.{BlockLocation, FileStatus, LocatedFileStatus, Path}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.star.exception.StarLakeErrors
import org.apache.spark.sql.star.utils.{DataFileInfo, TableInfo}
import org.apache.spark.sql.types.StructType

object MergePartitionedFileUtil {
  def notSplitFiles(sparkSession: SparkSession,
                    file: FileStatus,
                    filePath: Path,
                    partitionValues: InternalRow,
                    tableInfo: TableInfo,
                    fileInfo: Seq[DataFileInfo],
                    requestFilesSchemaMap: Map[String, StructType],
                    requestDataSchema: StructType,
                    requestPartitionFields: Array[String]): Seq[MergePartitionedFile] = {
    Seq(getPartitionedFile(
      sparkSession,
      file,
      filePath,
      partitionValues,
      tableInfo,
      fileInfo,
      requestFilesSchemaMap,
      requestDataSchema,
      requestPartitionFields))
  }

  def getPartitionedFile(sparkSession: SparkSession,
                         file: FileStatus,
                         filePath: Path,
                         partitionValues: InternalRow,
                         tableInfo: TableInfo,
                         fileInfo: Seq[DataFileInfo],
                         requestFilesSchemaMap: Map[String, StructType],
                         requestDataSchema: StructType,
                         requestPartitionFields: Array[String]): MergePartitionedFile = {
    val hosts = getBlockHosts(getBlockLocations(file), 0, file.getLen)

    val filePathStr = filePath
      .getFileSystem(sparkSession.sessionState.newHadoopConf())
      .makeQualified(filePath).toString
    val touchedFileInfo = fileInfo.find(f => filePathStr.equals(f.file_path))
      .getOrElse(throw StarLakeErrors.filePathNotFoundException(filePathStr, fileInfo.mkString(",")))

    val touchedFileSchema = requestFilesSchemaMap(touchedFileInfo.range_version).fieldNames

    val keyInfo = tableInfo.hash_partition_schema.map(f => {
      (touchedFileSchema.indexOf(f.name), f.dataType)
    })
    val fileSchemaInfo = requestFilesSchemaMap(touchedFileInfo.range_version).map(m => (m.name, m.dataType))
    val partitionSchemaInfo = requestPartitionFields.map(m => (m, tableInfo.range_partition_schema(m).dataType))
    val requestDataInfo = requestDataSchema.map(m => (m.name, m.dataType))

    MergePartitionedFile(
      partitionValues = partitionValues,
      filePath = filePath.toUri.toString,
      start = 0,
      length = file.getLen,
      qualifiedName = filePathStr,
      rangeKey = touchedFileInfo.range_key,
      keyInfo = keyInfo,
      resultSchema = requestDataInfo ++ partitionSchemaInfo,
      fileInfo = fileSchemaInfo ++ partitionSchemaInfo,
      writeVersion = touchedFileInfo.write_version,
      rangeVersion = touchedFileInfo.range_version,
      fileBucketId = touchedFileInfo.file_bucket_id,
      locations = hosts)
  }

  private def getBlockLocations(file: FileStatus): Array[BlockLocation] = file match {
    case f: LocatedFileStatus => f.getBlockLocations
    case f => Array.empty[BlockLocation]
  }

  // Given locations of all blocks of a single file, `blockLocations`, and an `(offset, length)`
  // pair that represents a segment of the same file, find out the block that contains the largest
  // fraction the segment, and returns location hosts of that block. If no such block can be found,
  // returns an empty array.
  private def getBlockHosts(blockLocations: Array[BlockLocation],
                            offset: Long,
                            length: Long): Array[String] = {
    val candidates = blockLocations.map {
      // The fragment starts from a position within this block. It handles the case where the
      // fragment is fully contained in the block.
      case b if b.getOffset <= offset && offset < b.getOffset + b.getLength =>
        b.getHosts -> (b.getOffset + b.getLength - offset).min(length)

      // The fragment ends at a position within this block
      case b if b.getOffset < offset + length && offset + length < b.getOffset + b.getLength =>
        b.getHosts -> (offset + length - b.getOffset)

      // The fragment fully contains this block
      case b if offset <= b.getOffset && b.getOffset + b.getLength <= offset + length =>
        b.getHosts -> b.getLength

      // The fragment doesn't intersect with this block
      case b =>
        b.getHosts -> 0L
    }.filter { case (hosts, size) =>
      size > 0L
    }

    if (candidates.isEmpty) {
      Array.empty[String]
    } else {
      val (hosts, _) = candidates.maxBy { case (_, size) => size }
      hosts
    }
  }
}