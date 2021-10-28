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

import com.engineplus.star.meta.MetaUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.EliminateSubqueryAliases
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, Expression, PredicateHelper, SubqueryExpression}
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LogicalPlan, Project}
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.datasources.v2.merge.MergeDeltaParquetScan
import org.apache.spark.sql.execution.datasources.v2.merge.parquet.batch.merge_operator.MergeOperator
import org.apache.spark.sql.execution.datasources.v2.{BatchScanExec, DataSourceV2Relation, DataSourceV2ScanRelation, FileScan}
import org.apache.spark.sql.star.catalog.StarLakeTableV2
import org.apache.spark.sql.star.exception.StarLakeErrors
import org.apache.spark.sql.star.rules.StarLakeRelation
import org.apache.spark.sql.star.sources.{StarLakeBaseRelation, StarLakeSourceUtils}
import org.apache.spark.sql.star.utils.{DataFileInfo, RelationTable}
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.util.Utils

import scala.collection.mutable.ArrayBuffer


object StarLakeUtils extends PredicateHelper {

  val MERGE_OP_COL = "_star_merge_col_name_"
  val MERGE_OP = "_star_merge_op_"

  lazy val USE_MATERIAL_REWRITE = "_star_use_material_rewrite_"


  def executeWithoutQueryRewrite[T](sparkSession: SparkSession)(f: => T): Unit ={
    sparkSession.conf.set(USE_MATERIAL_REWRITE, "false")
    f
    sparkSession.conf.set(USE_MATERIAL_REWRITE, "true")
  }

  def getClass(className: String): Class[_] = {
    Class.forName(className, true, Utils.getContextOrSparkClassLoader)
  }

  /** return async class */
  def getAsyncClass(className: String): (Boolean, Class[_]) = {
    try {
      val cls = Class.forName(className, true, Utils.getContextOrSparkClassLoader)
      (true, cls)
    } catch {
      case e: ClassNotFoundException => (false, null)
      case e: Exception => throw e
    }
  }

  /** Check whether this table is a StarTable based on information from the Catalog. */
  def isStarLakeTable(table: CatalogTable): Boolean = StarLakeSourceUtils.isStarLakeTable(table.provider)

  /**
    * Check whether the provided table name is a star table based on information from the Catalog.
    */
  def isStarLakeTable(spark: SparkSession, tableName: TableIdentifier): Boolean = {
    val catalog = spark.sessionState.catalog
    val tableIsNotTemporaryTable = !catalog.isTemporaryTable(tableName)
    val tableExists =
      (tableName.database.isEmpty || catalog.databaseExists(tableName.database.get)) &&
        catalog.tableExists(tableName)
    tableIsNotTemporaryTable && tableExists && isStarLakeTable(catalog.getTableMetadata(tableName))
  }

  /** Check if the provided path is the root or the children of a star table. */
  def isStarLakeTable(spark: SparkSession, path: Path): Boolean = {
    findTableRootPath(spark, path).isDefined
  }

  def isStarLakeTable(tablePath: String): Boolean = {
    val sparkSession = SparkSession.getActiveSession.getOrElse {
      throw new IllegalArgumentException("Could not find active SparkSession")
    }
    isStarLakeTable(sparkSession, new Path(MetaUtils.modifyTableString(tablePath)))
  }

  def findTableRootPath(spark: SparkSession, path: Path): Option[Path] = {
    var current_path = path
    while (current_path != null) {
      if (StarLakeSourceUtils.isStarLakeTableExists(current_path.toString)) {
        return Option(current_path)
      }
      current_path = current_path.getParent
    }
    None
  }

  /**
    * Partition the given condition into two sequence of conjunctive predicates:
    * - predicates that can be evaluated using metadata only.
    * - other predicates.
    */
  def splitMetadataAndDataPredicates(condition: Expression,
                                     partitionColumns: Seq[String],
                                     spark: SparkSession): (Seq[Expression], Seq[Expression]) = {
    splitConjunctivePredicates(condition).partition(
      isPredicateMetadataOnly(_, partitionColumns, spark))
  }

  /**
    * Check if condition can be evaluated using only metadata. In StarTable, this means the condition
    * only references partition columns and involves no subquery.
    */
  def isPredicateMetadataOnly(condition: Expression,
                              partitionColumns: Seq[String],
                              spark: SparkSession): Boolean = {
    isPredicatePartitionColumnsOnly(condition, partitionColumns, spark) &&
      !containsSubquery(condition)
  }

  def isPredicatePartitionColumnsOnly(condition: Expression,
                                      partitionColumns: Seq[String],
                                      spark: SparkSession): Boolean = {
    val nameEquality = spark.sessionState.analyzer.resolver
    condition.references.forall { r =>
      partitionColumns.exists(nameEquality(r.name, _))
    }
  }


  def containsSubquery(condition: Expression): Boolean = {
    SubqueryExpression.hasSubquery(condition)
  }


  /**
    * Replace the file index in a logical plan and return the updated plan.
    * It's a common pattern that, in StarTable commands, we use data skipping to determine a subset of
    * files that can be affected by the command, so we replace the whole-table file index in the
    * original logical plan with a new index of potentially affected files, while everything else in
    * the original plan, e.g., resolved references, remain unchanged.
    *
    * @param target the logical plan in which we replace the file index
    */

  def replaceFileIndex(target: LogicalPlan,
                       files: Seq[DataFileInfo]): LogicalPlan = {
    target transform {
      case l@LogicalRelation(egbr: StarLakeBaseRelation, _, _, _) =>
        l.copy(relation = egbr.copy(files = files)(egbr.sparkSession))
    }
  }

  def replaceFileIndexV2(target: LogicalPlan,
                         files: Seq[DataFileInfo]): LogicalPlan = {
    EliminateSubqueryAliases(target) match {
      case sr@DataSourceV2Relation(tbl: StarLakeTableV2, _, _, _, _) =>
        sr.copy(table = tbl.copy(userDefinedFileIndex = Option(BatchDataFileIndexV2(tbl.spark, tbl.snapshotManagement, files))))

      case _ => throw StarLakeErrors.starRelationIllegalException()
    }
  }


  /** Whether a path should be hidden for star-related file operations, such as cleanup. */
  def isHiddenDirectory(partitionColumnNames: Seq[String], pathName: String): Boolean = {
    // Names of the form partitionCol=[value] are partition directories, and should be
    // GCed even if they'd normally be hidden. The _db_index directory contains (bloom filter)
    // indexes and these must be GCed when the data they are tied to is GCed.
    (pathName.startsWith(".") || pathName.startsWith("_")) &&
      !partitionColumnNames.exists(c => pathName.startsWith(c ++ "="))
  }


  /**
    * parse relation table info for material view from spark plan
    * @param plan spark plan
    * @param array result array buffer
    */
  def parseRelationTableInfo(plan: SparkPlan, array: ArrayBuffer[RelationTable]): Unit = {
    plan match {
      case BatchScanExec(_, scan) =>
        val (fileIndex, filters) = scan match {
          case fileScan: FileScan if fileScan.fileIndex.isInstanceOf[StarLakeFileIndexV2] =>
            (fileScan.fileIndex.asInstanceOf[StarLakeFileIndexV2], fileScan.partitionFilters)

          case mergeScan: MergeDeltaParquetScan => (mergeScan.getFileIndex, mergeScan.getPartitionFilters)

          case _ => throw StarLakeErrors.materialViewBuildWithNonStarTableException()
        }

        val tableName = fileIndex.tableName
        val snapshot = fileIndex.snapshotManagement.snapshot
        val partitionInfo = PartitionFilter.partitionsForScan(snapshot, filters)
          .map(m => (m.range_id, m.read_version.toString))

        if(snapshot.getTableInfo.is_material_view){
          throw StarLakeErrors.materialViewBuildWithAnotherMaterialViewException()
        }

        array += RelationTable(tableName, snapshot.getTableInfo.table_id, partitionInfo)

      case p: SparkPlan if p.children.nonEmpty => p.children.foreach(parseRelationTableInfo(_, array))

      case _ => throw StarLakeErrors.materialViewBuildWithNonStarTableException()
    }
  }


//  /**
//    * parse material view/table info to help analysis
//    * @param plan
//    */
//  def parseMaterialTableInfo(plan: LogicalPlan, constructInfo: ConstructQueryInfo): Unit ={
//    //add output info
////    constructInfo.addOutputInfo(plan.output)
//    plan match {
//      case aggregate: Aggregate =>
//        //add aggregate info (group columns)
//        aggregate.groupingExpressions.foreach(exp =>
//          exp match {
//            case att: AttributeReference => constructInfo.addAggregateInfo(att)
//            case as: Alias =>
//              //add output info (Alias can only exists in output)
//              as.qualifier
//              exp.references
//              constructInfo.addAggregateInfo(as.child.asInstanceOf[AttributeReference])
//
//            case _ =>
//          })
//        //add output info
//        aggregate.aggregateExpressions.foreach(expression =>
//          expression.references)
//
////      case project: Project =>
////        project.projectList.foreach(p =>
////        p match {
////          case a @ AttributeReference =>
////        })
//
//    }
//  }


//  def parseMaterialTableInfo(plan: LogicalPlan, constructInfo: ConstructQueryInfo): Unit =


}


/**
  * Extractor Object for pulling out the table scan of a StarTable. It could be a full scan
  * or a partial scan.
  */
object StarLakeTable {
  def unapply(a: LogicalRelation): Option[StarLakeBaseRelation] = a match {
    case LogicalRelation(epbr: StarLakeBaseRelation, _, _, _) =>
      Some(epbr)
    case _ =>
      None
  }
}


/**
  * Extractor Object for pulling out the full table scan of a Star table.
  */
object StarLakeFullTable {
  def unapply(a: LogicalPlan): Option[StarLakeBaseRelation] = a match {
    case PhysicalOperation(_, filters, lr@StarLakeTable(epbr: StarLakeBaseRelation)) =>
      if (epbr.snapshotManagement.snapshot.isFirstCommit) return None
      if (filters.isEmpty) {
        Some(epbr)
      } else {
        throw new AnalysisException(
          s"Expect a full scan of Star sources, but found a partial scan. " +
            s"path:${epbr.snapshotManagement.table_name}")
      }
    // Convert V2 relations to V1 and perform the check
    case StarLakeRelation(lr) => unapply(lr)
    case _ => None
  }
}


object StarLakeTableRelationV2 {
  def unapply(plan: LogicalPlan): Option[StarLakeTableV2] = plan match {
    case DataSourceV2Relation(table: StarLakeTableV2, _, _, _, _) => Some(table)
    case DataSourceV2ScanRelation(DataSourceV2Relation(table: StarLakeTableV2, _, _, _, _), _, _) => Some(table)
    case _ => None
  }
}

object StarLakeTableV2ScanRelation {
  def unapply(plan: LogicalPlan): Option[DataSourceV2ScanRelation] = plan match {
    case dsv2@DataSourceV2Relation(t: StarLakeTableV2, _, _, _, _) => Some(createScanRelation(t, dsv2))
    case _ => None
  }

  def createScanRelation(table: StarLakeTableV2, v2Relation: DataSourceV2Relation): DataSourceV2ScanRelation = {
    DataSourceV2ScanRelation(
      v2Relation,
      table.newScanBuilder(v2Relation.options).build(),
      v2Relation.output)
  }
}


class MergeOpLong extends MergeOperator[Long] {
  override def mergeData(input: Seq[Long]): Long = {
    input.sum
  }
}