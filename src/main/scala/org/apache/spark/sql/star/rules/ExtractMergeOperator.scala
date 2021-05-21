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

package org.apache.spark.sql.star.rules

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.FunctionIdentifier
import org.apache.spark.sql.catalyst.expressions.{Alias, Cast, NamedExpression, ScalaUDF}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.star.StarLakeUtils
import org.apache.spark.sql.star.catalog.StarLakeTableV2
import org.apache.spark.sql.star.exception.StarLakeErrors
import org.apache.spark.sql.star.utils.AnalysisHelper

import scala.collection.mutable.ArrayBuffer

case class ExtractMergeOperator(sparkSession: SparkSession)
  extends Rule[LogicalPlan] with AnalysisHelper {
  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperatorsDown  {
    case p @ Project(projectList, relation @ DataSourceV2Relation(table: StarLakeTableV2, _,_,_,_)) =>
      val functionRegistry = sparkSession.sessionState.functionRegistry
      val arrBuf = new ArrayBuffer[(String, String)]()

      val newProjectList: Seq[NamedExpression] = projectList.map {
        case a@Alias(child: ScalaUDF, name) =>
          if (child.udfName.isDefined && child.udfName.get.startsWith(StarLakeUtils.MERGE_OP)) {
            val mergeOPName = child.udfName.get.replaceFirst(StarLakeUtils.MERGE_OP, "")
            val funInfo = functionRegistry.lookupFunction(FunctionIdentifier(mergeOPName)).get
            val mergeOpClassName = funInfo.getClassName
            val newChild = if (child.children.length == 1) {
              child.children.head match {
                case Cast(castChild, _, _) => castChild
                case _ => child.children.head
              }
            } else {
              child.children.head
            }
            assert(newChild.references.size == 1)
            arrBuf += ((StarLakeUtils.MERGE_OP_COL + newChild.references.head.name, mergeOpClassName))

            val tt = Alias(newChild, name)(a.exprId,a.qualifier,a.explicitMetadata)
            tt
          } else {
            a
          }

        case o => o
      }

      if (arrBuf.nonEmpty){
        val newTable = table.copy(mergeOperatorInfo = arrBuf.toMap)
        p.copy(projectList = newProjectList, child = relation.copy(table = newTable))
      }else{
        p
      }

    case p @ Project(projectList, filter @ Filter(_, relation @ DataSourceV2Relation(table: StarLakeTableV2, _,_,_,_))) =>
      val functionRegistry = sparkSession.sessionState.functionRegistry
      val arrBuf = new ArrayBuffer[(String, String)]()

      val newProjectList: Seq[NamedExpression] = projectList.map {
        case a @ Alias(child: ScalaUDF, name) =>
          if (child.udfName.isDefined && child.udfName.get.startsWith(StarLakeUtils.MERGE_OP)) {
            val mergeOPName = child.udfName.get.replaceFirst(StarLakeUtils.MERGE_OP, "")
            val funInfo = functionRegistry.lookupFunction(FunctionIdentifier(mergeOPName)).get
            val mergeOpClassName = funInfo.getClassName
            val newChild = if (child.children.length == 1) {
              child.children.head match {
                case Cast(castChild, _, _) => castChild
                case _ => child.children.head
              }
            } else {
              child.children.head
            }
            assert(newChild.references.size == 1)
            arrBuf += ((StarLakeUtils.MERGE_OP_COL + newChild.references.head.name, mergeOpClassName))

            val tt = Alias(newChild, name)(a.exprId,a.qualifier,a.explicitMetadata)
            tt
          } else {
            a
          }

        case o => o
      }

      if (arrBuf.nonEmpty){
        val newTable = table.copy(mergeOperatorInfo = arrBuf.toMap)
        p.copy(projectList = newProjectList, child = filter.copy(child = relation.copy(table = newTable)))
      }else{
        p
      }

  }


}


/**
  * A rule to check whether the merge operator udf exists
  */
case class NonMergeOperatorUDFCheck(spark: SparkSession)
  extends (LogicalPlan => Unit) {

  def apply(plan: LogicalPlan): Unit = {
    plan.foreach {
      case Project(projectList, _) =>
        projectList.foreach{
          case Alias(child: ScalaUDF, _) if child.udfName.isDefined && child.udfName.get.startsWith(StarLakeUtils.MERGE_OP)=>
            throw StarLakeErrors.useMergeOperatorForNonStarTableField(child.children.head.references.head.name)
          case _ =>
        }

      case _ => // OK
    }
  }
}