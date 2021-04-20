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

import java.util.{Locale, OptionalLong}

import com.engineplus.star.meta.MetaUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.parquet.hadoop.ParquetInputFormat
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.IO_WARNING_LARGEFILETHRESHOLD
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.execution.datasources.parquet.{ParquetReadSupport, ParquetWriteSupport}
import org.apache.spark.sql.execution.datasources.v2.merge.parquet.{MergeFilePartitionReaderFactory, MergeParquetPartitionReaderFactory}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.star.StarLakeFileIndexV2
import org.apache.spark.sql.star.utils.{DataFileInfo, TableInfo}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.util.SerializableConfiguration

abstract class MergeDeltaParquetScan(sparkSession: SparkSession,
                                     hadoopConf: Configuration,
                                     fileIndex: StarLakeFileIndexV2,
                                     dataSchema: StructType,
                                     readDataSchema: StructType,
                                     readPartitionSchema: StructType,
                                     pushedFilters: Array[Filter],
                                     options: CaseInsensitiveStringMap,
                                     tableInfo: TableInfo,
                                     partitionFilters: Seq[Expression] = Seq.empty,
                                     dataFilters: Seq[Expression] = Seq.empty)
  extends Scan with Batch
    with SupportsReportStatistics with Logging {
  def isSplittable(path: Path): Boolean = false

  lazy val fileInfo: Seq[DataFileInfo] = fileIndex.getFileInfo(partitionFilters)
    .map(f => if (f.is_base_file) {
      f.copy(write_version = 0)
    } else f)

  override def createReaderFactory(): PartitionReaderFactory = {
    val readDataSchemaAsJson = readDataSchema.json

    val requestedFields = readDataSchema.fieldNames
    val requestFilesSchema =
      fileInfo
        .groupBy(_.range_version)
        .map(m => {
          m._1 + "->" + StructType(
            (tableInfo.hash_partition_columns ++ m._2.head.file_exist_cols.split(","))
              .filter(requestedFields.contains)
              .map(c => tableInfo.schema(c))
          ).json
        }).mkString("|")

    hadoopConf.set(ParquetInputFormat.READ_SUPPORT_CLASS, classOf[ParquetReadSupport].getName)
    hadoopConf.set(
      ParquetReadSupport.SPARK_ROW_REQUESTED_SCHEMA,
      requestFilesSchema)
    hadoopConf.set(
      ParquetWriteSupport.SPARK_ROW_SCHEMA,
      readDataSchemaAsJson)
    hadoopConf.set(
      SQLConf.SESSION_LOCAL_TIMEZONE.key,
      sparkSession.sessionState.conf.sessionLocalTimeZone)
    hadoopConf.setBoolean(
      SQLConf.NESTED_SCHEMA_PRUNING_ENABLED.key,
      sparkSession.sessionState.conf.nestedSchemaPruningEnabled)
    hadoopConf.setBoolean(
      SQLConf.CASE_SENSITIVE.key,
      sparkSession.sessionState.conf.caseSensitiveAnalysis)

    ParquetWriteSupport.setSchema(readDataSchema, hadoopConf)

    // Sets flags for `ParquetToSparkSchemaConverter`
    hadoopConf.setBoolean(
      SQLConf.PARQUET_BINARY_AS_STRING.key,
      sparkSession.sessionState.conf.isParquetBinaryAsString)
    hadoopConf.setBoolean(
      SQLConf.PARQUET_INT96_AS_TIMESTAMP.key,
      sparkSession.sessionState.conf.isParquetINT96AsTimestamp)

    val broadcastedConf = sparkSession.sparkContext.broadcast(
      new SerializableConfiguration(hadoopConf))


    val canUseAsyncReader = tableInfo.table_name.startsWith("s3") || tableInfo.table_name.startsWith("oss")
    val asyncFactoryName = "org.apache.spark.sql.execution.datasources.v2.parquet.MergeParquetPartitionAsyncReaderFactory"
    val (hasAsyncClass, cls) = MetaUtils.getAsyncClass(asyncFactoryName)

    if (canUseAsyncReader && hasAsyncClass){
      logInfo("================  async merge scan   ==============================")

      cls.getConstructors()(0)
        .newInstance(sparkSession.sessionState.conf, broadcastedConf,
          dataSchema, readDataSchema, readPartitionSchema, pushedFilters)
        .asInstanceOf[MergeFilePartitionReaderFactory]

    }else{
      logInfo("================  merge scan no async  ==============================")

      MergeParquetPartitionReaderFactory(sparkSession.sessionState.conf, broadcastedConf,
        dataSchema, readDataSchema, readPartitionSchema, pushedFilters)
    }

  }

  protected def seqToString(seq: Seq[Any]): String = seq.mkString("[", ", ", "]")

  // Returns whether the two given arrays of [[Filter]]s are equivalent.
  protected def equivalentFilters(a: Array[Filter], b: Array[Filter]): Boolean = {
    a.sortBy(_.hashCode()).sameElements(b.sortBy(_.hashCode()))
  }


  override def hashCode(): Int = getClass.hashCode()

  override def description(): String = {
    super.description() + ", PushedFilters: " + seqToString(pushedFilters)
  }

  override def planInputPartitions(): Array[InputPartition] = {
    partitions.toArray
  }

  protected def partitions: Seq[MergeFilePartition] = {
    val selectedPartitions = fileIndex.listFiles(partitionFilters, dataFilters)
    val partitionAttributes = fileIndex.partitionSchema.toAttributes
    val attributeMap = partitionAttributes.map(a => normalizeName(a.name) -> a).toMap
    val readPartitionAttributes = readPartitionSchema.map { readField =>
      attributeMap.getOrElse(normalizeName(readField.name),
        throw new AnalysisException(s"Can't find required partition column ${readField.name} " +
          s"in partition schema ${fileIndex.partitionSchema}")
      )
    }
    lazy val partitionValueProject =
      GenerateUnsafeProjection.generate(readPartitionAttributes, partitionAttributes)
    val splitFiles = selectedPartitions.flatMap { partition =>
      // Prune partition values if part of the partition columns are not required.
      val partitionValues = if (readPartitionAttributes != partitionAttributes) {
        partitionValueProject(partition.values).copy()
      } else {
        partition.values
      }

      // produce requested schema
      val requestedFields = readDataSchema.fieldNames
      val requestFilesSchemaMap = fileInfo
        .groupBy(_.range_version)
        .map(m => {
          (m._1, StructType(
            (tableInfo.hash_partition_columns ++ m._2.head.file_exist_cols.split(","))
              .filter(requestedFields.contains)
              .map(c => tableInfo.schema(c))
          ))
        })

      partition.files.flatMap { file =>
        val filePath = file.getPath

        MergePartitionedFileUtil.notSplitFiles(
          sparkSession,
          file,
          filePath,
          partitionValues,
          tableInfo,
          fileInfo,
          requestFilesSchemaMap,
          readDataSchema,
          readPartitionSchema.fieldNames)
      }.toArray.sortBy(_.length)(implicitly[Ordering[Long]].reverse)
    }

    if (splitFiles.length == 1) {
      val path = new Path(splitFiles(0).filePath)
      if (!isSplittable(path) && splitFiles(0).length >
        sparkSession.sparkContext.getConf.get(IO_WARNING_LARGEFILETHRESHOLD)) {
        logWarning(s"Loading one large unsplittable file ${path.toString} with only one " +
          s"partition, the reason is: ${getFileUnSplittableReason(path)}")
      }
    }

    MergeFilePartition.getFilePartitions(splitFiles, tableInfo.bucket_num)
  }

  /**
    * If a file with `path` is unsplittable, return the unsplittable reason,
    * otherwise return `None`.
    */
  def getFileUnSplittableReason(path: Path): String = {
    assert(!isSplittable(path))
    "Merge parquet data Need Complete file"
  }

  private val isCaseSensitive = sparkSession.sessionState.conf.caseSensitiveAnalysis

  private def normalizeName(name: String): String = {
    if (isCaseSensitive) {
      name
    } else {
      name.toLowerCase(Locale.ROOT)
    }
  }

  override def estimateStatistics(): Statistics = {
    new Statistics {
      override def sizeInBytes(): OptionalLong = {
        val compressionFactor = sparkSession.sessionState.conf.fileCompressionFactor
        val size = (compressionFactor * fileIndex.sizeInBytes).toLong
        OptionalLong.of(size)
      }

      override def numRows(): OptionalLong = OptionalLong.empty()
    }
  }

  override def toBatch: Batch = this

  override def readSchema(): StructType =
    StructType(readDataSchema.fields ++ readPartitionSchema.fields)

}

case class OnePartitionMergeScan(sparkSession: SparkSession,
                                 hadoopConf: Configuration,
                                 fileIndex: StarLakeFileIndexV2,
                                 dataSchema: StructType,
                                 readDataSchema: StructType,
                                 readPartitionSchema: StructType,
                                 pushedFilters: Array[Filter],
                                 options: CaseInsensitiveStringMap,
                                 tableInfo: TableInfo,
                                 partitionFilters: Seq[Expression] = Seq.empty,
                                 dataFilters: Seq[Expression] = Seq.empty)
  extends MergeDeltaParquetScan(sparkSession,
    hadoopConf,
    fileIndex,
    dataSchema,
    readDataSchema,
    readPartitionSchema,
    pushedFilters,
    options,
    tableInfo,
    partitionFilters,
    dataFilters) {
  override def equals(obj: Any): Boolean = obj match {
    case p: OnePartitionMergeScan =>
      super.equals(p) && dataSchema == p.dataSchema && options == p.options &&
        equivalentFilters(pushedFilters, p.pushedFilters)
    case _ => false
  }
}


case class MultiPartitionMergeScan(sparkSession: SparkSession,
                                   hadoopConf: Configuration,
                                   fileIndex: StarLakeFileIndexV2,
                                   dataSchema: StructType,
                                   readDataSchema: StructType,
                                   readPartitionSchema: StructType,
                                   pushedFilters: Array[Filter],
                                   options: CaseInsensitiveStringMap,
                                   tableInfo: TableInfo,
                                   partitionFilters: Seq[Expression] = Seq.empty,
                                   dataFilters: Seq[Expression] = Seq.empty)
  extends MergeDeltaParquetScan(sparkSession,
    hadoopConf,
    fileIndex,
    dataSchema,
    readDataSchema,
    readPartitionSchema,
    pushedFilters,
    options,
    tableInfo,
    partitionFilters,
    dataFilters) {
  override def equals(obj: Any): Boolean = obj match {
    case p: MultiPartitionMergeScan =>
      super.equals(p) && dataSchema == p.dataSchema && options == p.options &&
        equivalentFilters(pushedFilters, p.pushedFilters)
    case _ => false
  }
}
