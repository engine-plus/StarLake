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

import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogUtils}
import org.apache.spark.sql.connector.catalog.TableCapability._
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.expressions.{FieldReference, IdentityTransform, Transform}
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.sources.{BaseRelation, Filter, InsertableRelation}
import org.apache.spark.sql.star._
import org.apache.spark.sql.star.commands.WriteIntoTable
import org.apache.spark.sql.star.sources.{StarLakeDataSource, StarLakeSourceUtils}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.{AnalysisException, DataFrame, SaveMode, SparkSession}

import scala.collection.JavaConverters._
import scala.collection.mutable

case class StarLakeTableV2(spark: SparkSession,
                           path: Path,
                           catalogTable: Option[CatalogTable] = None,
                           tableIdentifier: Option[String] = None,
                           userDefinedFileIndex: Option[StarLakeFileIndexV2] = None)
  extends Table with SupportsWrite with SupportsRead {

  private lazy val (rootPath, partitionFilters) =
    if (catalogTable.isDefined) {
      // Fast path for reducing path munging overhead
      (new Path(catalogTable.get.location), Nil)
    } else {
      StarLakeDataSource.parsePathIdentifier(spark, path.toString)
    }

  // The loading of the SnapshotManagement is lazy in order to reduce the amount of FileSystem calls,
  // in cases where we will fallback to the V1 behavior.
  lazy val snapshotManagement: SnapshotManagement = SnapshotManagement(rootPath)

  //  def getTableIdentifierIfExists: Option[TableIdentifier] = tableIdentifier.map(
  //    spark.sessionState.sqlParser.parseTableIdentifier)

  override def name(): String = catalogTable.map(_.identifier.unquotedString)
    .orElse(tableIdentifier)
    .getOrElse(s"star.`${snapshotManagement.table_name}`")

  private lazy val snapshot: Snapshot = snapshotManagement.updateSnapshot()

  override def schema(): StructType =
    StructType(snapshot.getTableInfo.data_schema ++ snapshot.getTableInfo.range_partition_schema)

  private lazy val dataSchema: StructType = snapshot.getTableInfo.data_schema

  private lazy val fileIndex: StarLakeFileIndexV2 = {
    if (userDefinedFileIndex.isDefined) {
      userDefinedFileIndex.get
    } else {
      DataFileIndexV2(spark, snapshotManagement)
    }
  }

  override def partitioning(): Array[Transform] = {
    snapshot.getTableInfo.range_partition_columns.map { col =>
      new IdentityTransform(new FieldReference(Seq(col)))
    }.toArray
  }

  override def properties(): java.util.Map[String, String] = {
    val base = new java.util.HashMap[String, String]()
    snapshot.getTableInfo.configuration.foreach { case (k, v) =>
      if (k != "path") {
        base.put(k, v)
      }
    }
    base.put(TableCatalog.PROP_PROVIDER, "star")
    base.put(TableCatalog.PROP_LOCATION, CatalogUtils.URIToString(path.toUri))
    //    Option(snapshot.getTableInfo.description).foreach(base.put(TableCatalog.PROP_COMMENT, _))
    base
  }

  override def capabilities(): java.util.Set[TableCapability] = Set(
    ACCEPT_ANY_SCHEMA, BATCH_READ, //BATCH_WRITE, OVERWRITE_DYNAMIC,
    V1_BATCH_WRITE, OVERWRITE_BY_FILTER, TRUNCATE
  ).asJava

  override def newScanBuilder(options: CaseInsensitiveStringMap): StarLakeScanBuilder =
    StarLakeScanBuilder(spark, fileIndex, schema(), dataSchema, options, snapshot.getTableInfo)

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    new WriteIntoTableBuilder(snapshotManagement, info.options)
  }

  /**
    * Creates a V1 BaseRelation from this Table to allow read APIs to go through V1 DataSource code
    * paths.
    */
  def toBaseRelation: BaseRelation = {
    val partitionPredicates = StarLakeDataSource.verifyAndCreatePartitionFilters(
      path.toString, snapshotManagement.snapshot, partitionFilters)

    snapshotManagement.createRelation(partitionPredicates)
  }


}

private class WriteIntoTableBuilder(snapshotManagement: SnapshotManagement,
                                    writeOptions: CaseInsensitiveStringMap)
  extends WriteBuilder with V1WriteBuilder with SupportsOverwrite with SupportsTruncate {

  private var forceOverwrite = false

  private val options =
    mutable.HashMap[String, String](writeOptions.asCaseSensitiveMap().asScala.toSeq: _*)

  override def truncate(): WriteIntoTableBuilder = {
    forceOverwrite = true
    this
  }

  override def overwrite(filters: Array[Filter]): WriteBuilder = {
    if (writeOptions.containsKey("replaceWhere")) {
      throw new AnalysisException(
        "You can't use replaceWhere in conjunction with an overwrite by filter")
    }
    options.put("replaceWhere", StarLakeSourceUtils.translateFilters(filters).sql)
    forceOverwrite = true
    this
  }

  override def buildForV1Write(): InsertableRelation = {
    new InsertableRelation {
      override def insert(data: DataFrame, overwrite: Boolean): Unit = {
        val session = data.sparkSession

        WriteIntoTable(
          snapshotManagement,
          if (forceOverwrite) SaveMode.Overwrite else SaveMode.Append,
          new StarLakeOptions(options.toMap, session.sessionState.conf),
          //          Nil,
          snapshotManagement.snapshot.getTableInfo.configuration,
          data).run(session)

        // TODO: Push this to Apache Spark
        // Re-cache all cached plans(including this relation itself, if it's cached) that refer
        // to this data source relation. This is the behavior for InsertInto
        session.sharedState.cacheManager.recacheByPlan(
          session, LogicalRelation(snapshotManagement.createRelation()))
      }
    }
  }
}
