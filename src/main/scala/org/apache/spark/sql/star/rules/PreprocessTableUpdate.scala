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

import org.apache.spark.sql.catalyst.analysis.EliminateSubqueryAliases
import org.apache.spark.sql.catalyst.expressions.SubqueryExpression
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, StarUpdate}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.star.catalog.StarLakeTableV2
import org.apache.spark.sql.star.commands.UpdateCommand
import org.apache.spark.sql.star.exception.StarLakeErrors
import org.apache.spark.sql.star.{StarLakeTableRelationV2, UpdateExpressionsSupport}

/**
  * Preprocesses the [[StarUpdate]] logical plan before converting it to [[UpdateCommand]].
  * - Adjusts the column order, which could be out of order, based on the destination table
  * - Generates expressions to compute the value of all target columns in StarTable, while taking
  * into account that the specified SET clause may only update some columns or nested fields of
  * columns.
  */
case class PreprocessTableUpdate(sqlConf: SQLConf)
  extends Rule[LogicalPlan] with UpdateExpressionsSupport {

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperators {
    case u: StarUpdate if u.resolved =>
      u.condition.foreach { cond =>
        if (SubqueryExpression.hasSubquery(cond)) {
          throw StarLakeErrors.subqueryNotSupportedException("UPDATE", cond)
        }
      }
      toCommand(u)
  }

  def toCommand(update: StarUpdate): UpdateCommand = {
    val snapshotManagement = EliminateSubqueryAliases(update.child) match {
      case StarLakeTableRelationV2(tbl: StarLakeTableV2) => tbl.snapshotManagement
      case o =>
        throw StarLakeErrors.notAnStarLakeSourceException("UPDATE", Some(o))
    }

    val targetColNameParts = update.updateColumns.map(StarUpdate.getTargetColNameParts(_))
    val alignedUpdateExprs = generateUpdateExpressions(
      update.child.output, targetColNameParts, update.updateExpressions, conf.resolver)
    UpdateCommand(snapshotManagement, update.child, alignedUpdateExprs, update.condition)
  }
}
