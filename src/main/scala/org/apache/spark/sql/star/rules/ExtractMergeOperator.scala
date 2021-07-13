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

import scala.collection.mutable

case class ExtractMergeOperator(sparkSession: SparkSession)
  extends Rule[LogicalPlan] with AnalysisHelper {

  private def getStarRelation(child: LogicalPlan): (Boolean, StarLakeTableV2) = {
    child match {
      case DataSourceV2Relation(table: StarLakeTableV2, _, _, _, _) => (true, table)
      case p: LogicalPlan if p.children.length == 1 => getStarRelation(p.children.head)
      case _ => (false, null)
    }
  }

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperatorsDown {
    case p@Project(list, child) if list.exists {
      case Alias(udf: ScalaUDF, _) if udf.udfName.isDefined && udf.udfName.get.startsWith(StarLakeUtils.MERGE_OP) => true
      case _ => false
    } =>
      val (hasStarRelation, starTable) = getStarRelation(child)
      if (hasStarRelation) {
        val functionRegistry = sparkSession.sessionState.functionRegistry
        val existMap = starTable.mergeOperatorInfo.getOrElse(Map.empty[String, String])

        val mergeOpMap = mutable.HashMap(existMap.toSeq: _*)

        val newProjectList: Seq[NamedExpression] = list.map {

          case a@Alias(udf: ScalaUDF, name) =>
            if (udf.udfName.isDefined && udf.udfName.get.startsWith(StarLakeUtils.MERGE_OP)) {
              val mergeOPName = udf.udfName.get.replaceFirst(StarLakeUtils.MERGE_OP, "")
              val funInfo = functionRegistry.lookupFunction(FunctionIdentifier(mergeOPName)).get
              val mergeOpClassName = funInfo.getClassName

              val newChild = if (udf.children.length == 1) {
                udf.children.head match {
                  case Cast(castChild, _, _) => castChild
                  case _ => udf.children.head
                }
              } else {
                udf.children.head
              }
              assert(newChild.references.size == 1)

              val key = StarLakeUtils.MERGE_OP_COL + newChild.references.head.name
              if (mergeOpMap.contains(key)) {
                throw StarLakeErrors.multiMergeOperatorException(newChild.references.head.name)
              }
              mergeOpMap.put(key, mergeOpClassName)

              val newAlias = Alias(newChild, name)(a.exprId, a.qualifier, a.explicitMetadata)
              newAlias
            } else {
              a
            }

          case o => o
        }

        if (mergeOpMap.nonEmpty) {
          starTable.mergeOperatorInfo = Option(mergeOpMap.toMap)
          p.copy(projectList = newProjectList)
        } else {
          p
        }
      } else {
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
        projectList.foreach {
          case Alias(child: ScalaUDF, _) if child.udfName.isDefined && child.udfName.get.startsWith(StarLakeUtils.MERGE_OP) =>
            throw StarLakeErrors.useMergeOperatorForNonStarTableField(child.children.head.references.head.name)
          case _ =>
        }

      case _ => // OK
    }
  }
}