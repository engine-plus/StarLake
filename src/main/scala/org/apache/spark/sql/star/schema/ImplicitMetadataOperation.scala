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

package org.apache.spark.sql.star.schema

import org.apache.spark.internal.Logging
import org.apache.spark.sql.star.TransactionCommit
import org.apache.spark.sql.star.exception.{MetadataMismatchErrorBuilder, StarLakeErrors}
import org.apache.spark.sql.star.utils.{PartitionUtils, TableInfo}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * A trait that writers into StarTable can extend to update the schema of the table.
  */
trait ImplicitMetadataOperation extends Logging {

  protected val canMergeSchema: Boolean
  protected val canOverwriteSchema: Boolean
  protected val rangePartitions: String
  protected val hashPartitions: String
  protected val hashBucketNum: Int
  protected val shortTableName: Option[String]

  private def transPartitionColums(partitionColumns: String): Seq[String] = {
    if (partitionColumns.equalsIgnoreCase("")) {
      Seq.empty[String]
    } else {
      partitionColumns.split(",").toSeq
    }
  }

  private def normalizePartitionColumns(spark: SparkSession,
                                        partitionCols: Seq[String],
                                        schema: StructType): Seq[String] = {
    partitionCols.map { columnName =>
      val colMatches = schema.filter(s => SchemaUtils.COL_RESOLVER(s.name, columnName))
      if (colMatches.length > 1) {
        throw StarLakeErrors.ambiguousPartitionColumnException(columnName, colMatches)
      } else if (colMatches.isEmpty) {
        throw StarLakeErrors.partitionColumnNotFoundException(columnName, schema.toAttributes)
      }
      colMatches.head.name
    }
  }

  protected final def updateMetadata(tc: TransactionCommit,
                                     data: Dataset[_],
                                     configuration: Map[String, String],
                                     isOverwriteMode: Boolean): Unit = {
    updateMetadata(
      data.sparkSession,
      tc,
      data.schema,
      configuration,
      isOverwriteMode
    )
  }

  protected final def updateMetadata(spark: SparkSession,
                                     tc: TransactionCommit,
                                     schema: StructType,
                                     configuration: Map[String, String],
                                     isOverwriteMode: Boolean): Unit = {
    val table_info = tc.tableInfo

    /**
      * If it is the first commit (e.g. create table), the parameters in OPTION are used;
      * otherwise, the parameters in META are used.
      */
    val (realRangeColumns, realHashColumns, realHashBucketNum) =
      if (tc.isFirstCommit) {
        (transPartitionColums(rangePartitions), transPartitionColums(hashPartitions), hashBucketNum)
      } else {
        //If the partition parameters are set and the table already exists, the settings must be the same as the table
        if (rangePartitions.nonEmpty && !table_info.range_column.equalsIgnoreCase(rangePartitions)) {
          throw StarLakeErrors.partitionColumnConflictException(table_info.range_column, rangePartitions, "Range")
        }
        if (hashPartitions.nonEmpty && !table_info.hash_column.equalsIgnoreCase(hashPartitions)) {
          throw StarLakeErrors.partitionColumnConflictException(table_info.hash_column, hashPartitions, "Hash")
        }
        if (hashBucketNum != -1 && table_info.bucket_num != hashBucketNum) {
          throw StarLakeErrors.hashBucketNumConflictException(table_info.bucket_num, hashBucketNum)
        }
        (transPartitionColums(table_info.range_column), transPartitionColums(table_info.hash_column), table_info.bucket_num)
      }

    if (shortTableName.isDefined){
      tc.setShortTableName(shortTableName.get)
    }

    val normalizedRangePartitionCols =
      normalizePartitionColumns(spark, realRangeColumns, schema)
    val normalizedHashPartitionCols =
      normalizePartitionColumns(spark, realHashColumns, schema)

    val dataSchema = StructType(schema.map {
      case StructField(name, dataType, nullable, metadata) =>
        if (normalizedRangePartitionCols.contains(name) || normalizedHashPartitionCols.contains(name)) {
          StructField(name, dataType, nullable = false, metadata)
        } else {
          StructField(name, dataType.asNullable, nullable = true, metadata)
        }
    })


    val mergedSchema = if (isOverwriteMode && canOverwriteSchema) {
      dataSchema
    } else {
      SchemaUtils.mergeSchemas(table_info.schema, dataSchema)
    }

    // Merged schema will contain additional columns at the end
    def isNewSchema: Boolean = table_info.schema != mergedSchema

    PartitionUtils.validatePartitionColumn(
      mergedSchema,
      normalizedRangePartitionCols ++ normalizedHashPartitionCols,
      // Star is case insensitive regarding internal column naming
      caseSensitive = false)

    if (tc.isFirstCommit) {
      if (dataSchema.isEmpty) {
        throw StarLakeErrors.emptyDataException
      }

      if (normalizedHashPartitionCols.nonEmpty) {
        if (realHashBucketNum == -1) {
          throw StarLakeErrors.hashBucketNumNotSetException()
        }
      }

      // If this is the first write, configure the metadata of the table.
      //todo: setting
      tc.updateTableInfo(
        TableInfo(
          table_name = table_info.table_name,
          table_id = table_info.table_id,
          table_schema = dataSchema.json,
          range_column = normalizedRangePartitionCols.mkString(","),
          hash_column = normalizedHashPartitionCols.mkString(","),
          bucket_num = realHashBucketNum,
          configuration = configuration))
    }
    else if (isOverwriteMode && canOverwriteSchema && isNewSchema) {
      val newTableInfo = tc.tableInfo.copy(
        table_schema = dataSchema.json
      )

      tc.updateTableInfo(newTableInfo)
    } else if (isNewSchema && canMergeSchema) {
      logInfo(s"New merged schema: ${mergedSchema.treeString}")

      tc.updateTableInfo(tc.tableInfo.copy(table_schema = mergedSchema.json))
    } else if (isNewSchema) {
      val errorBuilder = new MetadataMismatchErrorBuilder
      if (isNewSchema) {
        errorBuilder.addSchemaMismatch(tc.tableInfo.schema, dataSchema)
      }
      if (isOverwriteMode) {
        errorBuilder.addOverwriteBit()
      }
      errorBuilder.finalizeAndThrow()
    }
  }
}
