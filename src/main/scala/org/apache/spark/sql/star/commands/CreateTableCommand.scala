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

package org.apache.spark.sql.star.commands

import org.apache.hadoop.fs.Path
import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.CannotReplaceMissingTableException
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.star.exception.StarLakeErrors
import org.apache.spark.sql.star.schema.SchemaUtils
import org.apache.spark.sql.star.utils.{DataFileInfo, TableInfo}
import org.apache.spark.sql.star.{SnapshotManagement, StarLakeOptions, TransactionCommit}
import org.apache.spark.sql.types.StructType

/**
  * Single entry point for all write or declaration operations for Star tables accessed through
  * the table name.
  *
  * @param table            The table identifier for the Star table
  * @param existingTableOpt The existing table for the same identifier if exists
  * @param mode             The save mode when writing data. Relevant when the query is empty or set to Ignore
  *                         with `CREATE TABLE IF NOT EXISTS`.
  * @param query            The query to commit into the star table if it exist. This can come from
  *                - CTAS
  *                - saveAsTable
  */
case class CreateTableCommand(table: CatalogTable,
                              existingTableOpt: Option[CatalogTable],
                              mode: SaveMode,
                              query: Option[LogicalPlan],
                              operation: TableCreationModes.CreationMode = TableCreationModes.Create,
                              tableByPath: Boolean = false)
  extends RunnableCommand
    with Logging {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    assert(table.tableType != CatalogTableType.VIEW)
    assert(table.identifier.database.isDefined, "Database should've been fixed at analysis")
    // There is a subtle race condition here, where the table can be created by someone else
    // while this command is running. Nothing we can do about that though :(
    val tableExists = existingTableOpt.isDefined
    if (mode == SaveMode.Ignore && tableExists) {
      // Early exit on ignore
      return Nil
    } else if (mode == SaveMode.ErrorIfExists && tableExists) {
      throw new AnalysisException(s"Table ${table.identifier.quotedString} already exists.")
    }

    val tableWithLocation = if (tableExists) {
      val existingTable = existingTableOpt.get
      table.storage.locationUri match {
        case Some(location) if location.getPath != existingTable.location.getPath =>
          val tableName = table.identifier.quotedString
          throw new AnalysisException(
            s"The location of the existing table $tableName is " +
              s"`${existingTable.location}`. It doesn't match the specified location " +
              s"`${table.location}`.")
        case _ =>
      }
      table.copy(
        storage = existingTable.storage,
        tableType = existingTable.tableType)
    } else if (table.storage.locationUri.isEmpty) {
      // We are defining a new managed table
      assert(table.tableType == CatalogTableType.MANAGED)
      val loc = sparkSession.sessionState.catalog.defaultTablePath(table.identifier)
      table.copy(storage = table.storage.copy(locationUri = Some(loc)))
    } else {
      // We are defining a new external table
      assert(table.tableType == CatalogTableType.EXTERNAL)
      table
    }

    val isManagedTable = tableWithLocation.tableType == CatalogTableType.MANAGED
    val tableLocation = new Path(tableWithLocation.location)
    val fs = tableLocation.getFileSystem(sparkSession.sessionState.newHadoopConf())

    // external options to store replace and partition properties
    var externalOptions = Map.empty[String, String]
    if (table.partitionColumnNames.nonEmpty) {
      externalOptions ++= Map(StarLakeOptions.RANGE_PARTITIONS -> table.partitionColumnNames.mkString(","))
    }

    val options = new StarLakeOptions(
      table.storage.properties ++ externalOptions,
      sparkSession.sessionState.conf)

    val snapshotManagement = SnapshotManagement(fs.makeQualified(tableLocation))

    // don't support replace table
    operation match {
      case TableCreationModes.CreateOrReplace if !snapshotManagement.snapshot.isFirstCommit =>
        throw StarLakeErrors.operationNotSupportedException("replaceTable")
      case _ =>
    }


    val tc = snapshotManagement.startTransaction()

    var newMode: SaveMode = mode

    if (query.isDefined) {
      // If the mode is Ignore or ErrorIfExists, the table must not exist, or we would return
      // earlier. And the data should not exist either, to match the behavior of
      // Ignore/ErrorIfExists mode. This means the table path should not exist or is empty.
      if (mode == SaveMode.Ignore || mode == SaveMode.ErrorIfExists) {
        assert(!tableExists)
        // if table exists in meta, but has no data in table path
        // (this is unexpected, but may appear in test scene), we allow overwrite this table.
        if (!tc.isFirstCommit) {
          assertPathEmpty(sparkSession, tableWithLocation)
          newMode = SaveMode.Overwrite
        }
      }
      // We are either appending/overwriting with saveAsTable or creating a new table with CTAS

      val data = Dataset.ofRows(sparkSession, query.get)

      if (!isV1Writer) {
        replaceMetadataIfNecessary(tc, tableWithLocation, options, query.get.schema)
      }
      val (newFiles, deletedFiles) = WriteIntoTable(
        snapshotManagement,
        newMode,
        options,
        configuration = Map.empty, //table.properties,
        data).write(tc, sparkSession)

      tc.commit(newFiles, deletedFiles)
    } else {
      def createTableOrVerify(): Unit = {
        if (isManagedTable) {
          // When creating a managed table, the table path should not exist or is empty, or
          // users would be surprised to see the data, or see the data directory being dropped
          // after the table is dropped.
          assertPathEmpty(sparkSession, tableWithLocation)
        }

        // This is either a new table, or, we never defined the schema of the table. While it is
        // unexpected that `txn.metadata.schema` to be empty when txn.readVersion >= 0, we still
        // guard against it, in case of checkpoint corruption bugs.
        val noExistingMetadata = tc.isFirstCommit || tc.tableInfo.schema.isEmpty
        if (noExistingMetadata) {
          assertTableSchemaDefined(tableLocation, tableWithLocation)
          assertPathEmpty(sparkSession, tableWithLocation)
          // This is a user provided schema.
          // Doesn't come from a query, Follow nullability invariants.
          val newTableInfo = getProvidedTableInfo(tc, table, table.schema.json)

          tc.commit(Seq.empty[DataFileInfo], Seq.empty[DataFileInfo], newTableInfo)
        } else {
          verifyTableInfo(tc, tableWithLocation)
        }
      }
      // We are defining a table using the Create or Replace Table statements.
      operation match {
        case TableCreationModes.Create =>
          require(!tableExists, "Can't recreate a table when it exists")
          createTableOrVerify()

        case TableCreationModes.CreateOrReplace if !tableExists =>
          // If the table doesn't exist, CREATE OR REPLACE must provide a schema
          if (tableWithLocation.schema.isEmpty) {
            throw StarLakeErrors.schemaNotProvidedException
          }
          createTableOrVerify()
        case _ =>
          // When the operation is a REPLACE or CREATE OR REPLACE, then the schema shouldn't be
          // empty, since we'll use the entry to replace the schema
          if (tableWithLocation.schema.isEmpty) {
            throw StarLakeErrors.schemaNotProvidedException
          }
          // We need to replace
          replaceMetadataIfNecessary(tc, tableWithLocation, options, tableWithLocation.schema)
          // Truncate the table
          val operationTimestamp = System.currentTimeMillis()
          val removes = tc.filterFiles().map(_.expire(operationTimestamp))

          tc.commit(Seq.empty[DataFileInfo], removes)
      }
    }

    // We would have failed earlier on if we couldn't ignore the existence of the table
    // In addition, we just might using saveAsTable to append to the table, so ignore the creation
    // if it already exists.
    // Note that someone may have dropped and recreated the table in a separate location in the
    // meantime... Unfortunately we can't do anything there at the moment, because Hive sucks.
    val tableWithDefaultOptions = tableWithLocation.copy(
      schema = new StructType(),
      partitionColumnNames = Nil,
      tracksPartitionsInCatalog = true
    )

    updateCatalog(sparkSession, tableWithDefaultOptions)

    Nil

  }

  private def getProvidedTableInfo(tc: TransactionCommit,
                                   table: CatalogTable,
                                   schemaString: String): TableInfo = {
    TableInfo(table_name = tc.tableInfo.table_name,
      table_id = tc.tableInfo.table_id,
      table_schema = schemaString,
      range_column = table.partitionColumnNames.mkString(",")
      //      ,configuration = table.properties
    )
  }

  private def assertPathEmpty(sparkSession: SparkSession,
                              tableWithLocation: CatalogTable): Unit = {
    val path = new Path(tableWithLocation.location)
    val fs = path.getFileSystem(sparkSession.sessionState.newHadoopConf())
    // Verify that the table location associated with CREATE TABLE doesn't have any data. Note that
    // we intentionally diverge from this behavior w.r.t regular datasource tables (that silently
    // overwrite any previous data)
    if (fs.exists(path) && fs.listStatus(path).nonEmpty) {
      throw StarLakeErrors.failedCreateTableException(
        tableWithLocation.identifier.toString(),
        tableWithLocation.location.toString)
    }
  }


  private def assertTableSchemaDefined(path: Path, table: CatalogTable): Unit = {
    // Users did not specify the schema. We expect the schema exists in CatalogTable.
    if (table.schema.isEmpty) {
      if (table.tableType == CatalogTableType.EXTERNAL) {
        throw StarLakeErrors.createExternalTableWithoutSchemaException(
          path, table.identifier.quotedString)
      } else {
        throw StarLakeErrors.createManagedTableWithoutSchemaException(
          table.identifier.quotedString)
      }
    }
  }

  /**
    * Verify against our transaction tableInfo that the user specified the right metadata for the
    * table.
    */
  private def verifyTableInfo(tc: TransactionCommit,
                              tableDesc: CatalogTable): Unit = {
    val existingTableInfo = tc.tableInfo
    val path = new Path(tableDesc.location)

    // The star table already exists. If they give any configuration, we'll make sure it all matches.
    // Otherwise we'll just go with the metadata already present in the meta.
    // The schema compatibility checks will be made in `WriteIntoTable` for CreateTable
    // with a query
    if (!tc.isFirstCommit) {
      if (tableDesc.schema.nonEmpty) {
        // We check exact alignment on create table if everything is provided
        val differences = SchemaUtils.reportDifferences(existingTableInfo.schema, tableDesc.schema)
        if (differences.nonEmpty) {
          throw StarLakeErrors.createTableWithDifferentSchemaException(
            path, tableDesc.schema, existingTableInfo.schema, differences)
        }
      }

      // If schema is specified, we must make sure the partitioning matches, even the partitioning
      // is not specified.
      if (tableDesc.schema.nonEmpty &&
        tableDesc.partitionColumnNames != existingTableInfo.range_partition_columns) {
        throw StarLakeErrors.createTableWithDifferentPartitioningException(
          path, tableDesc.partitionColumnNames, existingTableInfo.range_partition_columns)
      }

      if (tableDesc.properties.nonEmpty && tableDesc.properties != existingTableInfo.configuration) {
        throw StarLakeErrors.createTableWithDifferentPropertiesException(
          path, tableDesc.properties, existingTableInfo.configuration)
      }
    }
  }

  /**
    * Similar to getOperation, here we disambiguate the catalog alterations we need to do based
    * on the table operation, and whether we have reached here through legacy code or DataSourceV2
    * code paths.
    */
  private def updateCatalog(spark: SparkSession, table: CatalogTable): Unit = operation match {
    case _ if tableByPath => // do nothing with the metastore if this is by path
    case TableCreationModes.Create =>
      spark.sessionState.catalog.createTable(
        table,
        ignoreIfExists = existingTableOpt.isDefined,
        validateLocation = false)
    case TableCreationModes.Replace if existingTableOpt.isDefined =>
      spark.sessionState.catalog.alterTable(table)
    case TableCreationModes.Replace =>
      val ident = Identifier.of(table.identifier.database.toArray, table.identifier.table)
      throw new CannotReplaceMissingTableException(ident)
    case TableCreationModes.CreateOrReplace if existingTableOpt.isDefined =>
      spark.sessionState.catalog.alterTable(table)
    case TableCreationModes.CreateOrReplace =>
      spark.sessionState.catalog.createTable(
        table,
        ignoreIfExists = false,
        validateLocation = false)
  }

  /**
    * With DataFrameWriterV2, methods like `replace()` or `createOrReplace()` mean that the
    * metadata of the table should be replaced. If overwriteSchema=false is provided with these
    * methods, then we will verify that the metadata match exactly.
    */
  private def replaceMetadataIfNecessary(tc: TransactionCommit,
                                         tableDesc: CatalogTable,
                                         options: StarLakeOptions,
                                         schema: StructType): Unit = {
    val isReplace = operation == TableCreationModes.CreateOrReplace ||
      operation == TableCreationModes.Replace
    // If a user explicitly specifies not to overwrite the schema, during a replace, we should
    // tell them that it's not supported
    val dontOverwriteSchema = options.options.contains(StarLakeOptions.OVERWRITE_SCHEMA_OPTION) &&
      !options.canOverwriteSchema
    if (isReplace && dontOverwriteSchema) {
      throw StarLakeErrors.illegalUsageException(StarLakeOptions.OVERWRITE_SCHEMA_OPTION, "replacing")
    }
    if (!tc.isFirstCommit && isReplace && !dontOverwriteSchema) {
      // When a table already exists, and we're using the DataFrameWriterV2 API to replace
      // or createOrReplace a table, we blindly overwrite the metadata.
      tc.updateTableInfo(getProvidedTableInfo(tc, table, schema.asNullable.json))
    }
  }

  /**
    * Horrible hack to differentiate between DataFrameWriterV1 and V2 so that we can decide
    * what to do with table metadata. In DataFrameWriterV1, mode("overwrite").saveAsTable,
    * behaves as a CreateOrReplace table, but we have asked for "overwriteSchema" as an
    * explicit option to overwrite partitioning or schema information. With DataFrameWriterV2,
    * the behavior asked for by the user is clearer: .createOrReplace(), which means that we
    * should overwrite schema and/or partitioning. Therefore we have this hack.
    */
  private def isV1Writer: Boolean = {
    Thread.currentThread().getStackTrace.exists(_.toString.contains(
      classOf[DataFrameWriter[_]].getCanonicalName + "."))
  }
}

object TableCreationModes {

  sealed trait CreationMode {
    def mode: SaveMode
  }

  case object Create extends CreationMode {
    override def mode: SaveMode = SaveMode.ErrorIfExists
  }

  case object CreateOrReplace extends CreationMode {
    override def mode: SaveMode = SaveMode.Overwrite
  }

  case object Replace extends CreationMode {
    override def mode: SaveMode = SaveMode.Overwrite
  }

}
