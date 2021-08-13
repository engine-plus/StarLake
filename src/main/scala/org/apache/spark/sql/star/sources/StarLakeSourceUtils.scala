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

import java.util.Locale

import com.engineplus.star.meta.{MetaCommit, MetaUtils, MetaVersion}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.sources.{BaseRelation, Filter, InsertableRelation, PrunedFilteredScan}
import org.apache.spark.sql.star.commands.WriteIntoTable
import org.apache.spark.sql.star.utils.{DataFileInfo, TableInfo}
import org.apache.spark.sql.star.{SnapshotManagement, StarLakeOptions}
import org.apache.spark.sql.types.StructType

object StarLakeSourceUtils {

  val NAME = "star"
  val SOURCENAME = "star"

  def isStarLakeDataSourceName(name: String): Boolean = {
    name.toLowerCase(Locale.ROOT) == NAME
  }

  def isStarLakeTableExists(path: String): Boolean = {
    val table_name = MetaUtils.modifyTableString(path)
    MetaVersion.isTableExists(table_name)
  }

  def isStarLakeShortTableNameExists(shortName: String): Boolean = {
    MetaVersion.isShortTableNameExists(shortName)._1
  }

  /** Check whether this table is a star table based on information from the Catalog. */
  def isStarLakeTable(provider: Option[String]): Boolean = {
    provider.exists(isStarLakeDataSourceName)
  }

  /** Creates Spark literals from a value exposed by the public Spark API. */
  private def createLiteral(value: Any): expressions.Literal = value match {
    case v: String => expressions.Literal.create(v)
    case v: Int => expressions.Literal.create(v)
    case v: Byte => expressions.Literal.create(v)
    case v: Short => expressions.Literal.create(v)
    case v: Long => expressions.Literal.create(v)
    case v: Double => expressions.Literal.create(v)
    case v: Float => expressions.Literal.create(v)
    case v: Boolean => expressions.Literal.create(v)
    case v: java.sql.Date => expressions.Literal.create(v)
    case v: java.sql.Timestamp => expressions.Literal.create(v)
    case v: BigDecimal => expressions.Literal.create(v)
  }

  /** Translates the public Spark Filter APIs into Spark internal expressions. */
  def translateFilters(filters: Array[Filter]): Expression = filters.map {
    case sources.EqualTo(attribute, value) =>
      expressions.EqualTo(UnresolvedAttribute(attribute), expressions.Literal.create(value))
    case sources.EqualNullSafe(attribute, value) =>
      expressions.EqualNullSafe(UnresolvedAttribute(attribute), expressions.Literal.create(value))
    case sources.GreaterThan(attribute, value) =>
      expressions.GreaterThan(UnresolvedAttribute(attribute), expressions.Literal.create(value))
    case sources.GreaterThanOrEqual(attribute, value) =>
      expressions.GreaterThanOrEqual(
        UnresolvedAttribute(attribute), expressions.Literal.create(value))
    case sources.LessThan(attribute, value) =>
      expressions.LessThanOrEqual(UnresolvedAttribute(attribute), expressions.Literal.create(value))
    case sources.LessThanOrEqual(attribute, value) =>
      expressions.LessThanOrEqual(UnresolvedAttribute(attribute), expressions.Literal.create(value))
    case sources.In(attribute, values) =>
      expressions.In(UnresolvedAttribute(attribute), values.map(createLiteral))
    case sources.IsNull(attribute) => expressions.IsNull(UnresolvedAttribute(attribute))
    case sources.IsNotNull(attribute) => expressions.IsNotNull(UnresolvedAttribute(attribute))
    case sources.Not(otherFilter) => expressions.Not(translateFilters(Array(otherFilter)))
    case sources.And(filter1, filter2) =>
      expressions.And(translateFilters(Array(filter1)), translateFilters(Array(filter2)))
    case sources.Or(filter1, filter2) =>
      expressions.Or(translateFilters(Array(filter1)), translateFilters(Array(filter2)))
    case sources.StringStartsWith(attribute, value) =>
      new expressions.Like(
        UnresolvedAttribute(attribute), expressions.Literal.create(s"$value%"))
    case sources.StringEndsWith(attribute, value) =>
      new expressions.Like(
        UnresolvedAttribute(attribute), expressions.Literal.create(s"%$value"))
    case sources.StringContains(attribute, value) =>
      new expressions.Like(
        UnresolvedAttribute(attribute), expressions.Literal.create(s"%$value%"))
    case sources.AlwaysTrue() => expressions.Literal.TrueLiteral
    case sources.AlwaysFalse() => expressions.Literal.FalseLiteral
  }.reduce(expressions.And)

}

case class StarLakeBaseRelation(files: Seq[DataFileInfo],
                                snapshotManagement: SnapshotManagement)(val sparkSession: SparkSession)
  extends BaseRelation with InsertableRelation with PrunedFilteredScan {

  override def sqlContext: SQLContext = sparkSession.sqlContext

  lazy val tableInfo: TableInfo = snapshotManagement.snapshot.getTableInfo

  override def schema: StructType = {
    tableInfo.schema
  }


  /**
    * Build the RDD to scan rows. todo: True predicates filter
    *
    * @param requiredColumns columns that are being requested by the requesting query
    * @param filters         filters that are being applied by the requesting query
    * @return RDD will all the results from star
    */
  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {

    //check and redo commit before read
    MetaCommit.checkAndRedoCommit(tableInfo.table_id)

    val predicts = filters.length match {
      case 0 => expressions.Literal(true)
      case _ => StarLakeSourceUtils.translateFilters(filters)
    }

    snapshotManagement
      .createDataFrame(files, requiredColumns, Option(predicts))
      .filter(Column(predicts))
      .select(requiredColumns.map(col): _*)
      .rdd
  }


  override def insert(data: DataFrame, overwrite: Boolean): Unit = {
    val mode = if (overwrite) SaveMode.Overwrite else SaveMode.Append
    WriteIntoTable(
      snapshotManagement,
      mode,
      new StarLakeOptions(Map.empty[String, String], sqlContext.conf),
      Map.empty,
      data).run(sparkSession)
  }


  /**
    * Returns the string representation of this StarLakeRelation
    *
    * @return Star + tableName of the relation
    */
  override def toString(): String = {
    "Star " + tableInfo.table_name
  }


}
