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

import org.apache.spark.Partition
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.types.DataType

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

/**
  * A part (i.e. "block") of a single file that should be read, along with partition column values
  * that need to be prepended to each row.
  *
  * @param partitionValues value of partition columns to be prepended to each row.
  * @param filePath        URI of the file to read
  * @param start           the beginning offset (in bytes) of the block.
  * @param length          number of bytes to read.
  * @param locations       locality information (list of nodes that have the data).
  */
case class MergePartitionedFile(partitionValues: InternalRow,
                                filePath: String,
                                start: Long,
                                length: Long,
                                qualifiedName: String,
                                rangeKey: String,
                                keyInfo: Seq[(Int, DataType)], //(key_index, dataType)
                                resultSchema: Seq[(String, DataType)], //all result columns name and type
                                fileSchema: Array[String], //file columns name
                                fileInfo: Seq[(String, DataType)], //file columns name and type
                                writeVersion: Long,
                                rangeVersion: String,
                                fileGroupId: Int, //hash split id
                                @transient locations: Array[String] = Array.empty) {
  override def toString: String = {
    s"path: $filePath, range: $start-${start + length}, partition values: $partitionValues"
  }
}

/**
  * A collection of file blocks that should be read as a single task
  * (possibly from multiple partitioned directories).
  */
case class MergeFilePartition(index: Int, files: Array[MergePartitionedFile], isSingleFile: Boolean)
  extends Partition with InputPartition {
  override def preferredLocations(): Array[String] = {
    // Computes total number of bytes can be retrieved from each host.
    val hostToNumBytes = mutable.HashMap.empty[String, Long]
    files.foreach { file =>
      file.locations.filter(_ != "localhost").foreach { host =>
        hostToNumBytes(host) = hostToNumBytes.getOrElse(host, 0L) + file.length
      }
    }

    // Takes the first 3 hosts with the most data to be retrieved
    hostToNumBytes.toSeq.sortBy {
      case (host, numBytes) => numBytes
    }.reverse.take(3).map {
      case (host, numBytes) => host
    }.toArray
  }
}

object MergeFilePartition extends Logging {

  def getFilePartitions(partitionedFiles: Seq[MergePartitionedFile],
                        bucketNum: Int): Seq[MergeFilePartition] = {

    val groupByPartition = partitionedFiles.groupBy(_.rangeKey)

    //for singe partition, use bucket id as MergeFilePartition index
    val filePartition = if (groupByPartition.size == 1) {
      val fileWithGroupId = groupByPartition.head._2
        .groupBy(_.fileGroupId).map(f => (f._1, f._2.toArray))

      val isSingleFile = groupByPartition.head._2.map(_.writeVersion).toSet.size == 1

      Seq.tabulate(bucketNum) { bucketId =>
        val files = fileWithGroupId.getOrElse(bucketId, Array.empty)
        assert(files.length == files.map(_.writeVersion).toSet.size,
          "Files has duplicate write version, it may has too many base file, have a check!")
        MergeFilePartition(bucketId, files, isSingleFile)
      }
    } else {
      var i = 0
      val partitions = new ArrayBuffer[MergeFilePartition]

      groupByPartition.foreach(p => {
        val isSingleFile = p._2.map(_.writeVersion).toSet.size == 1
        p._2.groupBy(_.fileGroupId).foreach(g => {
          val files = g._2.toArray
          assert(files.length == files.map(_.writeVersion).toSet.size,
            "Files has duplicate write version, it may has too many base files, have a check!")
          partitions += MergeFilePartition(i, files, isSingleFile)
          i = i + 1
        })
      })
      partitions
    }
    filePartition
  }
}












