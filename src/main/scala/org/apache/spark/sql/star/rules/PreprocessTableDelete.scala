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
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, StarDelete}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.star.StarLakeTableRelationV2
import org.apache.spark.sql.star.catalog.StarLakeTableV2
import org.apache.spark.sql.star.commands.DeleteCommand
import org.apache.spark.sql.star.exception.StarLakeErrors

/**
  * Preprocess the [[StarDelete]] plan to convert to [[DeleteCommand]].
  */
case class PreprocessTableDelete(sqlConf: SQLConf) extends Rule[LogicalPlan] {

  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan.resolveOperators {
      case d: StarDelete if d.resolved =>
        d.condition.foreach { cond =>
          if (SubqueryExpression.hasSubquery(cond)) {
            throw StarLakeErrors.subqueryNotSupportedException("DELETE", cond)
          }
        }
        toCommand(d)
    }
  }

  def toCommand(d: StarDelete): DeleteCommand = EliminateSubqueryAliases(d.child) match {
    case StarLakeTableRelationV2(tbl: StarLakeTableV2) =>
      DeleteCommand(tbl.snapshotManagement, d.child, d.condition)

    case o =>
      throw StarLakeErrors.notAnStarLakeSourceException("DELETE", Some(o))
  }
}
