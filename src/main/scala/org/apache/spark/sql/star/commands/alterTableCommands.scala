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

import org.apache.spark.sql.catalyst.analysis.{Resolver, UnresolvedAttribute}
import org.apache.spark.sql.catalyst.plans.logical.{IgnoreCachedData, QualifiedColType}
import org.apache.spark.sql.connector.catalog.TableChange.{After, ColumnPosition, First}
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.execution.datasources.parquet.ParquetSchemaConverter
import org.apache.spark.sql.star.catalog.StarLakeTableV2
import org.apache.spark.sql.star.exception.StarLakeErrors
import org.apache.spark.sql.star.schema.SchemaUtils
import org.apache.spark.sql.star.utils.DataFileInfo
import org.apache.spark.sql.star.{StarLakeConfig, TransactionCommit}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{AnalysisException, Row, SparkSession}

import scala.util.control.NonFatal

/**
  * A super trait for alter table commands that modify Star tables.
  */
trait AlterTableCommand extends Command {

  def table: StarLakeTableV2

  protected def startTransaction(): TransactionCommit = {
    val tc = table.snapshotManagement.startTransaction()
    if (tc.isFirstCommit) {
      throw StarLakeErrors.notAnStarLakeSourceException(table.name())
    }
    tc
  }
}

/**
  * A command that sets star table configuration.
  *
  * The syntax of this command is:
  * {{{
  *   ALTER TABLE table1 SET TBLPROPERTIES ('key1' = 'val1', 'key2' = 'val2', ...);
  * }}}
  */
case class AlterTableSetPropertiesCommand(
                                           table: StarLakeTableV2,
                                           configuration: Map[String, String])
  extends RunnableCommand with AlterTableCommand with IgnoreCachedData {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val tc = startTransaction()

    val tableInfo = tc.tableInfo
    val newTableInfo = tableInfo.copy(configuration = tableInfo.configuration ++ configuration)

    tc.commit(Seq.empty[DataFileInfo], Seq.empty[DataFileInfo], newTableInfo)
    Seq.empty[Row]

  }
}

/**
  * A command that unsets Star table configuration.
  * If ifExists is false, each individual key will be checked if it exists or not, it's a
  * one-by-one operation, not an all or nothing check. Otherwise, non-existent keys will be ignored.
  *
  * The syntax of this command is:
  * {{{
  *   ALTER TABLE table1 UNSET TBLPROPERTIES [IF EXISTS] ('key1', 'key2', ...);
  * }}}
  */
case class AlterTableUnsetPropertiesCommand(
                                             table: StarLakeTableV2,
                                             propKeys: Seq[String],
                                             ifExists: Boolean)
  extends RunnableCommand with AlterTableCommand with IgnoreCachedData {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val tc = startTransaction()
    val tableInfo = tc.tableInfo

    val normalizedKeys = StarLakeConfig.normalizeConfigKeys(propKeys)
    if (!ifExists) {
      normalizedKeys.foreach { k =>
        if (!tableInfo.configuration.contains(k)) {
          throw new AnalysisException(
            s"Attempted to unset non-existent property '$k' in table ${table.name()}")
        }
      }
    }

    val newConfiguration = tableInfo.configuration.filterNot {
      case (key, _) => normalizedKeys.contains(key)
    }
    val newTableInfo = tableInfo.copy(configuration = newConfiguration)
    tc.commit(Seq.empty[DataFileInfo], Seq.empty[DataFileInfo], newTableInfo)

    Seq.empty[Row]
  }
}

/**
  * A command that add columns to a star table.
  * The syntax of using this command in SQL is:
  * {{{
  *   ALTER TABLE table_identifier
  *   ADD COLUMNS (col_name data_type [COMMENT col_comment], ...);
  * }}}
  */
case class AlterTableAddColumnsCommand(
                                        table: StarLakeTableV2,
                                        colsToAddWithPosition: Seq[QualifiedColType])
  extends RunnableCommand with AlterTableCommand with IgnoreCachedData {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val tc = startTransaction()

    if (SchemaUtils.filterRecursively(
      StructType(colsToAddWithPosition.map {
        case QualifiedColTypeWithPosition(_, column, _) => column
      }), true)(!_.nullable).nonEmpty) {
      throw StarLakeErrors.operationNotSupportedException("NOT NULL in ALTER TABLE ADD COLUMNS")
    }

    // TODO: remove this after auto cache refresh is merged.
    table.tableIdentifier.foreach { identifier =>
      try sparkSession.catalog.uncacheTable(identifier) catch {
        case NonFatal(e) =>
          log.warn(s"Exception when attempting to uncache table $identifier", e)
      }
    }

    val tableInfo = tc.tableInfo
    val oldSchema = tableInfo.schema

    val resolver = sparkSession.sessionState.conf.resolver
    val newSchema = colsToAddWithPosition.foldLeft(oldSchema) {
      case (schema, QualifiedColTypeWithPosition(columnPath, column, None)) =>
        val (parentPosition, lastSize) =
          SchemaUtils.findColumnPosition(columnPath, schema, resolver)
        SchemaUtils.addColumn(schema, column, parentPosition :+ lastSize)
      case (schema, QualifiedColTypeWithPosition(columnPath, column, Some(_: First))) =>
        val (parentPosition, _) = SchemaUtils.findColumnPosition(columnPath, schema, resolver)
        SchemaUtils.addColumn(schema, column, parentPosition :+ 0)
      case (schema,
      QualifiedColTypeWithPosition(columnPath, column, Some(after: After))) =>
        val (prevPosition, _) =
          SchemaUtils.findColumnPosition(columnPath :+ after.column, schema, resolver)
        val position = prevPosition.init :+ (prevPosition.last + 1)
        SchemaUtils.addColumn(schema, column, position)
    }

    SchemaUtils.checkColumnNameDuplication(newSchema, "in adding columns")
    ParquetSchemaConverter.checkFieldNames(SchemaUtils.explodeNestedFieldNames(newSchema))

    val newTableInfo = tableInfo.copy(table_schema = newSchema.json)
    tc.commit(Seq.empty[DataFileInfo], Seq.empty[DataFileInfo], newTableInfo)

    Seq.empty[Row]
  }

  object QualifiedColTypeWithPosition {
    def unapply(col: QualifiedColType): Option[(Seq[String], StructField, Option[ColumnPosition])] = {
      val builder = new MetadataBuilder
      col.comment.foreach(builder.putString("comment", _))

      val field = StructField(col.name.last, col.dataType, col.nullable, builder.build())

      Some((col.name.init, field, col.position))
    }
  }

}

/**
  * A command to change the column for a Star table, support changing the comment of a column and
  * reordering columns.
  *
  * The syntax of using this command in SQL is:
  * {{{
  *   ALTER TABLE table_identifier
  *   CHANGE [COLUMN] column_old_name column_new_name column_dataType [COMMENT column_comment]
  *   [FIRST | AFTER column_name];
  * }}}
  */
case class AlterTableChangeColumnCommand(
                                          table: StarLakeTableV2,
                                          columnPath: Seq[String],
                                          columnName: String,
                                          newColumn: StructField,
                                          colPosition: Option[ColumnPosition])
  extends RunnableCommand with AlterTableCommand with IgnoreCachedData {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val tc = startTransaction()
    val tableInfo = tc.tableInfo
    val oldSchema = tableInfo.schema
    val resolver = sparkSession.sessionState.conf.resolver

    // Verify that the columnName provided actually exists in the schema
    SchemaUtils.findColumnPosition(columnPath :+ columnName, oldSchema, resolver)

    val newSchema = SchemaUtils.transformColumnsStructs(oldSchema, columnName) {
      case (`columnPath`, struct@StructType(fields), _) =>
        val oldColumn = struct(columnName)
        verifyColumnChange(struct(columnName), resolver)

        // Take the comment, nullability and data type from newField
        val newField = newColumn.getComment().map(oldColumn.withComment).getOrElse(oldColumn)
          .copy(
            dataType =
              SchemaUtils.changeDataType(oldColumn.dataType, newColumn.dataType, resolver),
            nullable = newColumn.nullable)

        // Replace existing field with new field
        val newFieldList = fields.map { field =>
          if (field.name == columnName) newField else field
        }

        // Reorder new field to correct position if necessary
        colPosition.map { position =>
          reorderFieldList(struct, newFieldList, newField, position, resolver)
        }.getOrElse(newFieldList.toSeq)

      case (_, _@StructType(fields), _) => fields
    }

    val newTableInfo = tableInfo.copy(table_schema = newSchema.json)
    tc.commit(Seq.empty[DataFileInfo], Seq.empty[DataFileInfo], newTableInfo)

    Seq.empty[Row]
  }

  /**
    * Reorder the given fieldList to place `field` at the given `position` in `fieldList`
    *
    * @param struct    The initial StructType with the original field at its original position
    * @param fieldList List of fields with the changed field in the original position
    * @param field     The field that is to be added
    * @param position  Position where the field is to be placed
    * @return Returns a new list of fields with the changed field in the new position
    */
  private def reorderFieldList(
                                struct: StructType,
                                fieldList: Array[StructField],
                                field: StructField,
                                position: ColumnPosition,
                                resolver: Resolver): Seq[StructField] = {
    val startIndex = struct.fieldIndex(columnName)
    val filtered = fieldList.filterNot(_.name == columnName)
    val newFieldList = position match {
      case _: First =>
        field +: filtered

      case after: After if after.column() == columnName =>
        filtered.slice(0, startIndex) ++
          Seq(field) ++
          filtered.slice(startIndex, filtered.length)

      case after: After =>
        val endIndex = filtered.indexWhere(i => resolver(i.name, after.column()))
        if (endIndex < 0) {
          throw StarLakeErrors.columnNotInSchemaException(after.column(), struct)
        }

        filtered.slice(0, endIndex + 1) ++
          Seq(field) ++
          filtered.slice(endIndex + 1, filtered.length)
    }
    newFieldList.toSeq
  }

  /**
    * Given two columns, verify whether replacing the original column with the new column is a valid
    * operation
    *
    * @param originalField The existing column
    */
  private def verifyColumnChange(
                                  originalField: StructField,
                                  resolver: Resolver): Unit = {

    originalField.dataType match {
      case same if same == newColumn.dataType =>
      // just changing comment or position so this is fine
      case s: StructType if s != newColumn.dataType =>
        val fieldName = UnresolvedAttribute(columnPath :+ columnName).name
        throw new AnalysisException(
          s"Cannot update ${table.name()} field $fieldName type: " +
            s"update a struct by adding, deleting, or updating its fields")
      case m: MapType if m != newColumn.dataType =>
        val fieldName = UnresolvedAttribute(columnPath :+ columnName).name
        throw new AnalysisException(
          s"Cannot update ${table.name()} field $fieldName type: " +
            s"update a map by updating $fieldName.key or $fieldName.value")
      case a: ArrayType if a != newColumn.dataType =>
        val fieldName = UnresolvedAttribute(columnPath :+ columnName).name
        throw new AnalysisException(
          s"Cannot update ${table.name()} field $fieldName type: " +
            s"update the element by updating $fieldName.element")
      case _: AtomicType =>
      // update is okay
      case o =>
        throw new AnalysisException(s"Cannot update ${table.name()} field of type $o")
    }

    if (columnName != newColumn.name ||
      SchemaUtils.canChangeDataType(originalField.dataType, newColumn.dataType, resolver,
        columnPath :+ originalField.name).nonEmpty ||
      (originalField.nullable && !newColumn.nullable)) {
      throw StarLakeErrors.alterTableChangeColumnException(
        s"'${UnresolvedAttribute(columnPath :+ originalField.name).name}' with type " +
          s"'${originalField.dataType}" +
          s" (nullable = ${originalField.nullable})'",
        s"'${UnresolvedAttribute(Seq(newColumn.name)).name}' with type " +
          s"'${newColumn.dataType}" +
          s" (nullable = ${newColumn.nullable})'")
    }
  }
}

/**
  * A command to replace columns for a StarTable, support changing the comment of a column,
  * reordering columns, and loosening nullabilities.
  *
  * The syntax of using this command in SQL is:
  * {{{
  *   ALTER TABLE table_identifier REPLACE COLUMNS (col_spec[, col_spec ...]);
  * }}}
  */
case class AlterTableReplaceColumnsCommand(
                                            table: StarLakeTableV2,
                                            columns: Seq[StructField])
  extends RunnableCommand with AlterTableCommand with IgnoreCachedData {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val tc = startTransaction()

    val tableInfo = tc.tableInfo
    val existingSchema = tableInfo.schema

    val resolver = sparkSession.sessionState.conf.resolver
    val changingSchema = StructType(columns)

    SchemaUtils.canChangeDataType(existingSchema, changingSchema, resolver).foreach { operation =>
      throw StarLakeErrors.alterTableReplaceColumnsException(
        existingSchema, changingSchema, operation)
    }

    val newSchema = SchemaUtils.changeDataType(existingSchema, changingSchema, resolver)
      .asInstanceOf[StructType]

    SchemaUtils.checkColumnNameDuplication(newSchema, "in replacing columns")
    ParquetSchemaConverter.checkFieldNames(SchemaUtils.explodeNestedFieldNames(newSchema))

    val newTableInfo = tableInfo.copy(table_schema = newSchema.json)
    tc.commit(Seq.empty[DataFileInfo], Seq.empty[DataFileInfo], newTableInfo)

    Seq.empty[Row]
  }
}
