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

package org.apache.spark.sql.star.catalog

import com.engineplus.star.meta.{MetaCommit, MetaUtils}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.connector.read.{Scan, SupportsPushDownFilters}
import org.apache.spark.sql.execution.datasources.parquet.{ParquetFilters, SparkToParquetSchemaConverter}
import org.apache.spark.sql.execution.datasources.v2.FileScanBuilder
import org.apache.spark.sql.execution.datasources.v2.merge.{MultiPartitionMergeBucketScan, MultiPartitionMergeScan, OnePartitionMergeBucketScan}
import org.apache.spark.sql.execution.datasources.v2.parquet.{BucketParquetScan, ParquetScan}
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.star.{StarLakeFileIndexV2, StarLakeUtils}
import org.apache.spark.sql.star.sources.{StarLakeSQLConf, StarLakeSourceUtils}
import org.apache.spark.sql.star.utils.TableInfo
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConverters._


case class StarLakeScanBuilder(sparkSession: SparkSession,
                               fileIndex: StarLakeFileIndexV2,
                               schema: StructType,
                               dataSchema: StructType,
                               options: CaseInsensitiveStringMap,
                               tableInfo: TableInfo)
  extends FileScanBuilder(sparkSession, fileIndex, dataSchema) with SupportsPushDownFilters with Logging {
  lazy val hadoopConf = {
    val caseSensitiveMap = options.asCaseSensitiveMap.asScala.toMap
      .filter(!_._1.startsWith(StarLakeUtils.MERGE_OP_COL))
    // Hadoop Configurations are case sensitive.
    sparkSession.sessionState.newHadoopConfWithOptions(caseSensitiveMap)
  }

  lazy val pushedParquetFilters = {
    val sqlConf = sparkSession.sessionState.conf
    val pushDownDate = sqlConf.parquetFilterPushDownDate
    val pushDownTimestamp = sqlConf.parquetFilterPushDownTimestamp
    val pushDownDecimal = sqlConf.parquetFilterPushDownDecimal
    val pushDownStringStartWith = sqlConf.parquetFilterPushDownStringStartWith
    val pushDownInFilterThreshold = sqlConf.parquetFilterPushDownInFilterThreshold
    val isCaseSensitive = sqlConf.caseSensitiveAnalysis
    val parquetSchema =
      new SparkToParquetSchemaConverter(sparkSession.sessionState.conf).convert(schema)
    val parquetFilters = new ParquetFilters(parquetSchema, pushDownDate, pushDownTimestamp,
      pushDownDecimal, pushDownStringStartWith, pushDownInFilterThreshold, isCaseSensitive)
    parquetFilters.convertibleFilters(this.filters).toArray
  }

  override protected val supportsNestedSchemaPruning: Boolean = true

  private var filters: Array[Filter] = Array.empty

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    this.filters = filters
    this.filters
  }

  def parseFilter(): Expression = {
    val predicts = filters.length match {
      case 0 => expressions.Literal(true)
      case _ => StarLakeSourceUtils.translateFilters(filters)
    }

    predicts
  }

  // Note: for Parquet, the actual filter push down happens in [[ParquetPartitionReaderFactory]].
  // It requires the Parquet physical schema to determine whether a filter is convertible.
  // All filters that can be converted to Parquet are pushed down.
  override def pushedFilters: Array[Filter] = pushedParquetFilters

  //note: hash partition columns must be last
  def mergeReadDataSchema(): StructType = {
    StructType((readDataSchema() ++ tableInfo.hash_partition_schema).distinct)
  }

  override def build(): Scan = {
    //check and redo commit before read
    MetaCommit.checkAndRedoCommit(tableInfo.table_id)

    val fileInfo = fileIndex.getFileInfo(Seq(parseFilter())).groupBy(_.range_partitions)
    val onlyOnePartition = fileInfo.size <= 1
    val hasNoDeltaFile = fileInfo.forall(f => f._2.forall(_.is_base_file))
    val validFormat = tableInfo.table_name.startsWith("s3") || tableInfo.table_name.startsWith("oss")
    val canUseAsyncReader = validFormat && sparkSession.sessionState.conf.getConf(StarLakeSQLConf.ASYNC_READER_ENABLE)


    if (tableInfo.hash_partition_columns.isEmpty) {
      parquetScan(canUseAsyncReader)
    }
    else if (onlyOnePartition) {
      if (hasNoDeltaFile) {
        BucketParquetScan(sparkSession, hadoopConf, fileIndex, dataSchema, readDataSchema(),
          readPartitionSchema(), pushedParquetFilters, options, tableInfo, Seq(parseFilter()))
      } else {
        OnePartitionMergeBucketScan(sparkSession, hadoopConf, fileIndex, dataSchema, mergeReadDataSchema(),
          readPartitionSchema(), pushedParquetFilters, options, tableInfo, Seq(parseFilter()))
      }
    }
    else {
      if (sparkSession.sessionState.conf
        .getConf(StarLakeSQLConf.BUCKET_SCAN_MULTI_PARTITION_ENABLE)) {
        MultiPartitionMergeBucketScan(sparkSession, hadoopConf, fileIndex, dataSchema, mergeReadDataSchema(),
          readPartitionSchema(), pushedParquetFilters, options, tableInfo, Seq(parseFilter()))
      } else if (hasNoDeltaFile) {
        parquetScan(canUseAsyncReader)
      } else {
        MultiPartitionMergeScan(sparkSession, hadoopConf, fileIndex, dataSchema, mergeReadDataSchema(),
          readPartitionSchema(), pushedParquetFilters, options, tableInfo, Seq(parseFilter()))
      }

    }
  }


  def parquetScan(canUseAsyncReader: Boolean): Scan = {
    val asyncFactoryName = "org.apache.spark.sql.execution.datasources.v2.parquet.AsyncParquetScan"
    val (hasAsyncClass, cls) = StarLakeUtils.getAsyncClass(asyncFactoryName)

    if (canUseAsyncReader && hasAsyncClass) {
      logInfo("======================  async scan   ========================")

      val constructor = cls.getConstructors()(0)
      constructor.newInstance(sparkSession, hadoopConf, fileIndex, dataSchema, readDataSchema(),
        readPartitionSchema(), pushedParquetFilters, options, Seq(parseFilter()), Seq.empty)
        .asInstanceOf[Scan]

    } else {
      logInfo("======================  scan no async  ========================")

      ParquetScan(sparkSession, hadoopConf, fileIndex, dataSchema, readDataSchema(),
        readPartitionSchema(), pushedParquetFilters, options, Seq(parseFilter()))
    }
  }

}