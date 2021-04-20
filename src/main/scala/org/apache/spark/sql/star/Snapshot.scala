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

import com.engineplus.star.meta.{DataOperation, MetaUtils}
import org.apache.spark.sql.execution.datasources.FileFormat
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.star.utils.{DataFileInfo, PartitionFilterInfo, PartitionInfo, TableInfo}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}


class Snapshot(table_info: TableInfo,
               partition_info_arr: Array[PartitionInfo],
               is_first_commit: Boolean = false
              ) {
  lazy val spark: SparkSession = SparkSession.active

  def getTableName: String = table_info.table_name

  def getTableInfo: TableInfo = table_info

  lazy val allDataInfo: Array[DataFileInfo] = DataOperation.getTableDataInfo(partition_info_arr)

  lazy val allPartitionFilterInfo: Seq[PartitionFilterInfo] = {
    partition_info_arr
      .map(part =>
        PartitionFilterInfo(
          part.range_id,
          part.range_value,
          MetaUtils.getPartitionMapFromKey(part.range_value)))
  }

  def allDataInfoDS: Dataset[DataFileInfo] = {
    import spark.implicits._
    spark.sparkContext.parallelize(allDataInfo).toDS()
  }

  def allPartitionFilterInfoDF: DataFrame = {
    import spark.implicits._
    spark.sparkContext.parallelize(allPartitionFilterInfo).toDF()
  }

  lazy val sizeInBytes: Long = allDataInfo.map(_.size).sum

  /** Return the underlying Spark `FileFormat` of the StarTable. */
  def fileFormat: FileFormat = new ParquetFileFormat()

  def getConfiguration: Map[String, String] = table_info.configuration

  def isFirstCommit: Boolean = is_first_commit

  def getPartitionInfoArray: Array[PartitionInfo] = partition_info_arr

}
