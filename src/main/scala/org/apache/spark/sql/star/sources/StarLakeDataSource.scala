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

package org.apache.spark.sql.star.sources

import com.engineplus.star.meta.MetaCommit
import org.apache.hadoop.fs.Path
import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{EqualTo, Expression, Literal}
import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.execution.datasources.DataSourceUtils
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.sources._
import org.apache.spark.sql.star._
import org.apache.spark.sql.star.catalog.StarLakeTableV2
import org.apache.spark.sql.star.commands.WriteIntoTable
import org.apache.spark.sql.star.exception.StarLakeErrors
import org.apache.spark.sql.star.utils.PartitionUtils
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.json4s.jackson.Serialization
import org.json4s.{Formats, NoTypeHints}

class StarLakeDataSource
  extends DataSourceRegister
    with RelationProvider
    with CreatableRelationProvider
    with StreamSinkProvider
    with TableProvider
    with Logging {


  override def shortName(): String = {
    StarLakeSourceUtils.NAME
  }

  override def createSink(sqlContext: SQLContext,
                          parameters: Map[String, String],
                          partitionColumns: Seq[String],
                          outputMode: OutputMode): Sink = {
    val path = parameters.getOrElse("path", {
      throw StarLakeErrors.pathNotSpecifiedException
    })

    val snapshot = SnapshotManagement(path).snapshot
    val tableInfo = snapshot.getTableInfo
    //before sink, checkAndRedoCommit first
    MetaCommit.checkAndRedoCommit(snapshot)

    //update mode can only be used with hash partition
    if (outputMode == OutputMode.Update()) {
      if (tableInfo.hash_column.isEmpty && parameters.getOrElse("hashpartitions", "").isEmpty) {
        throw StarLakeErrors.outputModeNotSupportedException(getClass.getName, outputMode)
      }
    }

    //add partition info to parameters to support partitionBy in streaming sink
    val newParam = if (partitionColumns.nonEmpty) {
      parameters ++ Map(
        DataSourceUtils.PARTITIONING_COLUMNS_KEY ->
          DataSourceUtils.encodePartitioningColumns(partitionColumns)
      )
    } else {
      parameters
    }

    val options = new StarLakeOptions(newParam, sqlContext.sparkSession.sessionState.conf)
    new StarLakeSink(sqlContext, new Path(path), outputMode, options)
  }


  override def createRelation(sqlContext: SQLContext,
                              mode: SaveMode,
                              parameters: Map[String, String],
                              data: DataFrame): BaseRelation = {
    val path = parameters.getOrElse("path", {
      throw StarLakeErrors.pathNotSpecifiedException
    })
    val snapshot_manage = SnapshotManagement(path)

    WriteIntoTable(
      snapshot_manage,
      mode = mode,
      new StarLakeOptions(parameters, sqlContext.sparkSession.sessionState.conf),
      Map.empty,
      data).run(sqlContext.sparkSession)

    snapshot_manage.createRelation()
  }


  override def createRelation(sqlContext: SQLContext,
                              parameters: Map[String, String]): BaseRelation = {
    val path = parameters.getOrElse("path", {
      throw StarLakeErrors.pathNotSpecifiedException
    })

    StarLakeTableV2(sqlContext.sparkSession, new Path(path)).toBaseRelation
  }


  def inferSchema: StructType = new StructType() // empty

  override def inferSchema(options: CaseInsensitiveStringMap): StructType = inferSchema

  override def getTable(schema: StructType,
                        partitioning: Array[Transform],
                        properties: java.util.Map[String, String]): Table = {
    val options = new CaseInsensitiveStringMap(properties)
    val path = options.get("path")
    if (path == null) throw StarLakeErrors.pathNotSpecifiedException
    StarLakeTableV2(SparkSession.active, new Path(path))
  }


}


object StarLakeDataSource extends Logging {

  private implicit val formats: Formats = Serialization.formats(NoTypeHints)

  def encodePartitioningColumns(columns: Seq[String]): String = {
    Serialization.write(columns)
  }

  def decodePartitioningColumns(str: String): Seq[String] = {
    Serialization.read[Seq[String]](str)
  }

  /**
    * For StarTable, we allow certain magic to be performed through the paths that are provided by users.
    * Normally, a user specified path should point to the root of a StarTable. However, some users
    * are used to providing specific partition values through the path, because of how expensive it
    * was to perform partition discovery before. We treat these partition values as logical partition
    * filters, if a table does not exist at the provided path.
    *
    * In addition, we allow users to provide time travel specifications through the path. This is
    * provided after an `@` symbol after a path followed by a time specification in
    * `yyyyMMddHHmmssSSS` format, or a version number preceded by a `v`.
    *
    * This method parses these specifications and returns these modifiers only if a path does not
    * really exist at the provided path. We first parse out the time travel specification, and then
    * the partition filters. For example, a path specified as:
    * /some/path/partition=1@v1234
    * will be parsed into `/some/path` with filters `partition=1` and a time travel spec of version
    * 1234.
    *
    * @return A tuple of the root path of the StarTable, partition filters, and time travel options
    */
  def parsePathIdentifier(spark: SparkSession,
                          path: String): (Path, Seq[(String, String)]) = {

    val hadoopPath = new Path(path)
    val rootPath = StarLakeUtils.findTableRootPath(spark, hadoopPath).getOrElse {
      throw StarLakeErrors.tableNotExistsException(path)
    }

    val partitionFilters = if (rootPath != hadoopPath) {
      logInfo(
        """
          |WARNING: loading partitions directly with star is not recommended.
          |If you are trying to read a specific partition, use a where predicate.
          |
          |CORRECT: spark.read.format("star").load("/data").where("part=1")
          |INCORRECT: spark.read.format("star").load("/data/part=1")
        """.stripMargin)

      val fragment = hadoopPath.toString.substring(rootPath.toString.length() + 1)
      try {
        PartitionUtils.parsePathFragmentAsSeq(fragment)
      } catch {
        case _: ArrayIndexOutOfBoundsException =>
          throw StarLakeErrors.partitionPathParseException(fragment)
      }
    } else {
      Nil
    }

    (rootPath, partitionFilters)
  }


  /**
    * Verifies that the provided partition filters are valid and returns the corresponding
    * expressions.
    */
  def verifyAndCreatePartitionFilters(userPath: String,
                                      snapshot: Snapshot,
                                      partitionFilters: Seq[(String, String)]): Seq[Expression] = {
    if (partitionFilters.nonEmpty) {
      val table_info = snapshot.getTableInfo

      val badColumns = partitionFilters.map(_._1).filterNot(table_info.range_partition_columns.contains)
      if (badColumns.nonEmpty) {
        val fragment = partitionFilters.map(f => s"${f._1}=${f._2}").mkString("/")
        throw StarLakeErrors.partitionPathInvolvesNonPartitionColumnException(badColumns, fragment)
      }

      val filters = partitionFilters.map { case (key, value) =>
        // Nested fields cannot be partitions, so we pass the key as a identifier
        EqualTo(UnresolvedAttribute(Seq(key)), Literal(value))
      }
      val files = PartitionFilter.partitionsForScan(snapshot, filters)
      if (files.isEmpty) {
        throw StarLakeErrors.tableNotExistsException(userPath)
      }
      filters
    } else {
      Nil
    }
  }


}