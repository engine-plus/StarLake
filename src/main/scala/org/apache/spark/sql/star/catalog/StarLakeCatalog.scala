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

import java.util

import com.engineplus.star.meta.MetaVersion
import com.engineplus.star.tables.StarTable
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{NoSuchDatabaseException, NoSuchNamespaceException, NoSuchTableException, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, QualifiedColType}
import org.apache.spark.sql.connector.catalog.TableCapability._
import org.apache.spark.sql.connector.catalog.TableChange._
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.expressions.{BucketTransform, FieldReference, IdentityTransform, Transform}
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, V1WriteBuilder, WriteBuilder}
import org.apache.spark.sql.execution.datasources.parquet.ParquetSchemaConverter
import org.apache.spark.sql.execution.datasources.{DataSource, PartitioningUtils}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.sources.InsertableRelation
import org.apache.spark.sql.star.commands._
import org.apache.spark.sql.star.exception.StarLakeErrors
import org.apache.spark.sql.star.sources.StarLakeSourceUtils
import org.apache.spark.sql.star.{StarLakeConfig, StarLakeUtils}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.{AnalysisException, DataFrame, SparkSession}

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * A Catalog extension which can properly handle the interaction between the HiveMetaStore and
  * star tables. It delegates all operations DataSources other than star to the SparkCatalog.
  */
class StarLakeCatalog(val spark: SparkSession) extends DelegatingCatalogExtension
  with StagingTableCatalog
  with SupportsPathIdentifier {

  def this() = {
    this(SparkSession.active)
  }

  /**
    * Creates a star table
    *
    * @param ident       The identifier of the table
    * @param schema      The schema of the table
    * @param partitions  The partition transforms for the table
    * @param properties  The table properties. Right now it also includes write options for backwards
    *                    compatibility
    * @param sourceQuery A query if this CREATE request came from a CTAS or RTAS
    * @param operation   The specific table creation mode, whether this is a Create/Replace/Create or
    *                    Replace
    */
  private def createStarLakeTable(ident: Identifier,
                                  schema: StructType,
                                  partitions: Array[Transform],
                                  properties: util.Map[String, String],
                                  sourceQuery: Option[LogicalPlan],
                                  operation: TableCreationModes.CreationMode): Table = {
    // These two keys are properties in data source v2 but not in v1, so we have to filter
    // them out. Otherwise property consistency checks will fail.
    val tableProperties = properties.asScala.filterKeys {
      case TableCatalog.PROP_LOCATION => false
      case TableCatalog.PROP_PROVIDER => false
      case TableCatalog.PROP_COMMENT => false
      case TableCatalog.PROP_OWNER => false
      case "path" => false
      case _ => true
    }
    // START: This entire block until END is a copy-paste from V2SessionCatalog
    val (partitionColumns, maybeBucketSpec) = convertTransforms(partitions)
    val isByPath = isPathIdentifier(ident)
    val location = if (isByPath) {
      Option(ident.name())
    } else {
      Option(properties.get("location"))
    }
    val storage = DataSource.buildStorageFormatFromOptions(tableProperties.toMap)
      .copy(locationUri = location.map(CatalogUtils.stringToURI))
    val tableType =
      if (location.isDefined) CatalogTableType.EXTERNAL else CatalogTableType.MANAGED

    val tableDesc = new CatalogTable(
      identifier = TableIdentifier(ident.name(), ident.namespace().lastOption),
      tableType = tableType,
      storage = storage,
      schema = schema,
      provider = Some("star"),
      partitionColumnNames = partitionColumns,
      bucketSpec = maybeBucketSpec,
      properties = tableProperties.toMap,
      comment = Option(properties.get("comment")))
    // END: copy-paste from the super method finished.

    val withDb = verifyTableAndSolidify(tableDesc, None)

    ParquetSchemaConverter.checkFieldNames(tableDesc.schema.fieldNames)
    CreateTableCommand(
      withDb,
      getExistingTableIfExists(tableDesc),
      operation.mode,
      sourceQuery,
      operation,
      tableByPath = isByPath).run(spark)

    loadTable(ident)
  }

  override def loadTable(ident: Identifier): Table = {
    try {
      super.loadTable(ident) match {
        case v1: V1Table if StarLakeUtils.isStarLakeTable(v1.catalogTable) =>
          StarLakeTableV2(
            spark,
            new Path(v1.catalogTable.location),
            catalogTable = Some(v1.catalogTable),
            tableIdentifier = Some(ident.toString))

        case o => o
      }
    } catch {
      case _: NoSuchDatabaseException | _: NoSuchNamespaceException | _: NoSuchTableException
        if isPathIdentifier(ident) =>
        StarLakeTableV2(
          spark,
          new Path(ident.name()))
      case _: NoSuchDatabaseException | _: NoSuchNamespaceException | _: NoSuchTableException
        if isNameIdentifier(ident) =>
        val tableName = MetaVersion.getTableNameFromShortTableName(ident.name)
        StarLakeTableV2(
          spark,
          new Path(tableName))
    }
  }

  private def getProvider(properties: util.Map[String, String]): String = {
    Option(properties.get("provider"))
      .getOrElse(spark.sessionState.conf.getConf(SQLConf.DEFAULT_DATA_SOURCE_NAME))
  }

  override def createTable(
                            ident: Identifier,
                            schema: StructType,
                            partitions: Array[Transform],
                            properties: util.Map[String, String]): Table = {
    if (StarLakeSourceUtils.isStarLakeDataSourceName(getProvider(properties))) {
      createStarLakeTable(
        ident, schema, partitions, properties, sourceQuery = None, TableCreationModes.Create)
    } else {
      super.createTable(ident, schema, partitions, properties)
    }
  }

  override def stageReplace(
                             ident: Identifier,
                             schema: StructType,
                             partitions: Array[Transform],
                             properties: util.Map[String, String]): StagedTable = {
    if (StarLakeSourceUtils.isStarLakeDataSourceName(getProvider(properties))) {
      throw StarLakeErrors.operationNotSupportedException("replaceTable")
    } else {
      super.dropTable(ident)
      BestEffortStagedTable(
        ident,
        super.createTable(ident, schema, partitions, properties),
        this)
    }
  }

  override def stageCreateOrReplace(
                                     ident: Identifier,
                                     schema: StructType,
                                     partitions: Array[Transform],
                                     properties: util.Map[String, String]): StagedTable = {
    if (StarLakeSourceUtils.isStarLakeDataSourceName(getProvider(properties))) {

      new StagedStarLakeTableV2(
        ident, schema, partitions, properties, TableCreationModes.CreateOrReplace)
    } else {
      try super.dropTable(ident) catch {
        case _: NoSuchTableException => // this is fine
      }
      BestEffortStagedTable(
        ident,
        super.createTable(ident, schema, partitions, properties),
        this)
    }
  }

  override def stageCreate(
                            ident: Identifier,
                            schema: StructType,
                            partitions: Array[Transform],
                            properties: util.Map[String, String]): StagedTable = {
    if (StarLakeSourceUtils.isStarLakeDataSourceName(getProvider(properties))) {
      new StagedStarLakeTableV2(ident, schema, partitions, properties, TableCreationModes.Create)
    } else {
      BestEffortStagedTable(
        ident,
        super.createTable(ident, schema, partitions, properties),
        this)
    }
  }

  // Copy of V2SessionCatalog.convertTransforms, which is private.
  private def convertTransforms(partitions: Seq[Transform]): (Seq[String], Option[BucketSpec]) = {
    val identityCols = new mutable.ArrayBuffer[String]
    var bucketSpec = Option.empty[BucketSpec]

    partitions.map {
      case IdentityTransform(FieldReference(Seq(col))) =>
        identityCols += col

      case BucketTransform(numBuckets, FieldReference(Seq(col))) =>
        bucketSpec = Some(BucketSpec(numBuckets, col :: Nil, Nil))

      case transform =>
        throw StarLakeErrors.operationNotSupportedException(s"Partitioning by expressions")
    }

    (identityCols, bucketSpec)
  }

  /** Performs checks on the parameters provided for table creation for a Star table. */
  private def verifyTableAndSolidify(tableDesc: CatalogTable,
                                     query: Option[LogicalPlan]): CatalogTable = {

    if (tableDesc.bucketSpec.isDefined) {
      throw StarLakeErrors.operationNotSupportedException("Bucketing", tableDesc.identifier)
    }

    val ori_schema = query.map { plan =>
      assert(tableDesc.schema.isEmpty, "Can't specify table schema in CTAS.")
      plan.schema.asNullable
    }.getOrElse(tableDesc.schema)

    val schema = StructType(ori_schema.map {
      case StructField(name, dataType, nullable, metadata) =>
        if (tableDesc.partitionColumnNames.contains(name)) {
          StructField(name, dataType, nullable = false, metadata)
        } else {
          StructField(name, dataType, nullable, metadata)
        }
    })


    PartitioningUtils.validatePartitionColumn(
      schema,
      tableDesc.partitionColumnNames,
      caseSensitive = false) // star is case insensitive

    val validatedConfigurations = StarLakeConfig.validateConfigurations(tableDesc.properties)

    val db = tableDesc.identifier.database.getOrElse(catalog.getCurrentDatabase)
    val tableIdentWithDB = tableDesc.identifier.copy(database = Some(db))
    tableDesc.copy(
      identifier = tableIdentWithDB,
      schema = schema,
      properties = validatedConfigurations)
  }

  /** Checks if a table already exists for the provided identifier. */
  private def getExistingTableIfExists(table: CatalogTable): Option[CatalogTable] = {
    // If this is a path identifier, we cannot return an existing CatalogTable. The Create command
    // will check the file system itself
    if (isPathIdentifier(table)) return None
    val tableExists = catalog.tableExists(table.identifier)

    if (tableExists) {
      val oldTable = catalog.getTableMetadata(table.identifier)
      if (oldTable.tableType == CatalogTableType.VIEW) {
        throw new AnalysisException(s"${table.identifier} is a view. You may not write data into a view.")
      }
      if (!StarLakeSourceUtils.isStarLakeTable(oldTable.provider)) {
        throw new AnalysisException(s"${table.identifier} is not a Star table. Please drop this " +
          "table first if you would like to create it with StarLake.")
      }
      Some(oldTable)
    } else {
      None
    }
  }

  /**
    * A staged star table, which creates a HiveMetaStore entry and appends data if this was a
    * CTAS/RTAS command. We have a ugly way of using this API right now, but it's the best way to
    * maintain old behavior compatibility between Databricks Runtime and OSS Star Lake.
    */
  private class StagedStarLakeTableV2(
                                       ident: Identifier,
                                       override val schema: StructType,
                                       val partitions: Array[Transform],
                                       override val properties: util.Map[String, String],
                                       operation: TableCreationModes.CreationMode) extends StagedTable with SupportsWrite {

    private var asSelectQuery: Option[DataFrame] = None
    private var writeOptions: Map[String, String] = properties.asScala.toMap

    override def commitStagedChanges(): Unit = {
      createStarLakeTable(
        ident,
        schema,
        partitions,
        writeOptions.asJava,
        asSelectQuery.map(_.queryExecution.analyzed),
        operation)
    }

    override def name(): String = ident.name()

    override def abortStagedChanges(): Unit = {}

    override def capabilities(): util.Set[TableCapability] = Set(V1_BATCH_WRITE).asJava

    override def newWriteBuilder(info: LogicalWriteInfo): V1WriteBuilder = {
      // TODO: We now pass both properties and options into CreateTableCommand, because
      // it wasn't supported in the initial APIs, but with DFWriterV2, we should actually separate
      // them
      val combinedProps = info.options.asCaseSensitiveMap().asScala ++ properties.asScala
      writeOptions = combinedProps.toMap
      new StarLakeV1WriteBuilder
    }

    /*
     * WriteBuilder for creating a star table.
     */
    private class StarLakeV1WriteBuilder extends WriteBuilder with V1WriteBuilder {
      override def buildForV1Write(): InsertableRelation = {
        new InsertableRelation {
          override def insert(data: DataFrame, overwrite: Boolean): Unit = {
            asSelectQuery = Option(data)
          }
        }
      }
    }

  }

  override def alterTable(ident: Identifier, changes: TableChange*): Table = {
    val table = loadTable(ident) match {
      case starTable: StarLakeTableV2 => starTable
      case _ => return super.alterTable(ident, changes: _*)
    }

    // We group the table changes by their type, since star applies each in a separate action.
    // We also must define an artificial type for SetLocation, since data source V2 considers
    // location just another property but it's special in catalog tables.
    class SetLocation {}
    val grouped = changes.groupBy {
      case s: SetProperty if s.property() == "location" => classOf[SetLocation]
      case c => c.getClass
    }

    val columnUpdates = new mutable.HashMap[Seq[String], (StructField, Option[ColumnPosition])]()

    grouped.foreach {
      case (t, newColumns) if t == classOf[AddColumn] =>
        AlterTableAddColumnsCommand(
          table,
          newColumns.asInstanceOf[Seq[AddColumn]].map { col =>
            QualifiedColType(
              col.fieldNames(),
              col.dataType(),
              col.isNullable,
              Option(col.comment()),
              Option(col.position()))
          }).run(spark)

      case (t, newProperties) if t == classOf[SetProperty] =>
        AlterTableSetPropertiesCommand(
          table,
          StarLakeConfig.validateConfigurations(
            newProperties.asInstanceOf[Seq[SetProperty]].map { prop =>
              prop.property() -> prop.value()
            }.toMap)
        ).run(spark)

      case (t, oldProperties) if t == classOf[RemoveProperty] =>
        AlterTableUnsetPropertiesCommand(
          table,
          oldProperties.asInstanceOf[Seq[RemoveProperty]].map(_.property()),
          // Data source V2 REMOVE PROPERTY is always IF EXISTS.
          ifExists = true).run(spark)

      case (t, columnChanges) if classOf[ColumnChange].isAssignableFrom(t) =>
        def getColumn(fieldNames: Seq[String]): (StructField, Option[ColumnPosition]) = {
          columnUpdates.getOrElseUpdate(fieldNames, {
            val schema = table.snapshotManagement.snapshot.getTableInfo.schema
            val colName = UnresolvedAttribute(fieldNames).name
            val fieldOpt = schema.findNestedField(fieldNames, includeCollections = true,
              spark.sessionState.conf.resolver)
              .map(_._2)
            val field = fieldOpt.getOrElse {
              throw new AnalysisException(
                s"Couldn't find column $colName in:\n${schema.treeString}")
            }
            field -> None
          })
        }

        columnChanges.foreach {
          case comment: UpdateColumnComment =>
            val field = comment.fieldNames()
            val (oldField, pos) = getColumn(field)
            columnUpdates(field) = oldField.withComment(comment.newComment()) -> pos

          case dataType: UpdateColumnType =>
            val field = dataType.fieldNames()
            val (oldField, pos) = getColumn(field)
            columnUpdates(field) = oldField.copy(dataType = dataType.newDataType()) -> pos

          case position: UpdateColumnPosition =>
            val field = position.fieldNames()
            val (oldField, pos) = getColumn(field)
            columnUpdates(field) = oldField -> Option(position.position())

          case nullability: UpdateColumnNullability =>
            val field = nullability.fieldNames()
            val (oldField, pos) = getColumn(field)
            columnUpdates(field) = oldField.copy(nullable = nullability.nullable()) -> pos

          case rename: RenameColumn =>
            val field = rename.fieldNames()
            val (oldField, pos) = getColumn(field)
            columnUpdates(field) = oldField.copy(name = rename.newName()) -> pos

          case other =>
            throw new UnsupportedOperationException("Unrecognized column change " +
              s"${other.getClass}. You may be running an out of date Star version.")
        }

      case (t, locations) if t == classOf[SetLocation] =>
        throw StarLakeErrors.operationNotSupportedException("ALTER TABLE xxx SET LOCATION '/xxx'")
    }

    columnUpdates.foreach { case (fieldNames, (newField, newPositionOpt)) =>
      AlterTableChangeColumnCommand(
        table,
        fieldNames.dropRight(1),
        fieldNames.last,
        newField,
        newPositionOpt).run(spark)
    }

    loadTable(ident)
  }

  // We want our catalog to handle star, therefore for other data sources that want to be
  // created, we just have this wrapper StagedTable to only drop the table if the commit fails.
  private case class BestEffortStagedTable(
                                            ident: Identifier,
                                            table: Table,
                                            catalog: TableCatalog) extends StagedTable with SupportsWrite {
    override def abortStagedChanges(): Unit = catalog.dropTable(ident)

    override def commitStagedChanges(): Unit = {}

    // Pass through
    override def name(): String = table.name()

    override def schema(): StructType = table.schema()

    override def partitioning(): Array[Transform] = table.partitioning()

    override def capabilities(): util.Set[TableCapability] = table.capabilities()

    override def properties(): util.Map[String, String] = table.properties()

    override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = table match {
      case supportsWrite: SupportsWrite => supportsWrite.newWriteBuilder(info)
      case _ => throw new AnalysisException(s"Table implementation does not support writes: $name")
    }
  }

  override def dropTable(ident: Identifier): Boolean = {
    if (isPathIdentifier(ident)){
      StarTable.forPath(ident.name()).dropTable()
    }else if (isNameIdentifier(ident)){
      StarTable.forName(ident.name()).dropTable()
    }else{
      super.dropTable(ident)
    }
  }


}

/**
  * A trait for handling table access through star.`/some/path`. This is a stop-gap solution
  * until PathIdentifiers are implemented in Apache Spark.
  */
trait SupportsPathIdentifier extends TableCatalog {
  self: StarLakeCatalog =>

  private def supportSQLOnFile: Boolean = spark.sessionState.conf.runSQLonFile

  protected lazy val catalog: SessionCatalog = spark.sessionState.catalog

  private def hasStarLakeNamespace(ident: Identifier): Boolean = {
    ident.namespace().length == 1 && StarLakeSourceUtils.isStarLakeDataSourceName(ident.namespace().head)
  }

  protected def isPathIdentifier(ident: Identifier): Boolean = {
    // Should be a simple check of a special PathIdentifier class in the future
    try {
      supportSQLOnFile && hasStarLakeNamespace(ident) && new Path(ident.name()).isAbsolute
    } catch {
      case _: IllegalArgumentException => false
    }
  }

  protected def isNameIdentifier(ident: Identifier): Boolean = {
    hasStarLakeNamespace(ident) && MetaVersion.isShortTableNameExists(ident.name())._1
  }

  protected def isPathIdentifier(table: CatalogTable): Boolean = {
    isPathIdentifier(Identifier.of(table.identifier.database.toArray, table.identifier.table))
  }

  override def tableExists(ident: Identifier): Boolean = {
    if (isPathIdentifier(ident)) {
      StarLakeSourceUtils.isStarLakeTableExists(ident.name())
    }else if(isNameIdentifier(ident)){
      StarLakeSourceUtils.isStarLakeShortTableNameExists(ident.name())
    } else {
      super.tableExists(ident)
    }
  }
}
