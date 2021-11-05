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
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.expressions.{Literal, SubqueryExpression}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.star.commands.UpsertCommand
import org.apache.spark.sql.star.exception.StarLakeErrors
import org.apache.spark.sql.star.{StarLakeTableRelationV2, UpdateExpressionsSupport}

case class PreprocessTableUpsert(sqlConf: SQLConf)
  extends Rule[LogicalPlan] with UpdateExpressionsSupport {

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperators {
    case m: StarUpsert if m.resolved => apply(m)
  }

  def apply(upsert: StarUpsert): UpsertCommand = {
    val StarUpsert(target, source, condition, migratedSchema) = upsert

    def checkCondition(conditionString: String, conditionName: String): Unit = {
      val cond = conditionString match {
        case "" => Literal(true)
        case _ => expr(conditionString).expr
      }

      if (!cond.deterministic) {
        throw StarLakeErrors.nonDeterministicNotSupportedException(
          s"$conditionName condition of UPSERT operation", cond)
      }
      if (cond.find(_.isInstanceOf[AggregateExpression]).isDefined) {
        throw StarLakeErrors.aggsNotSupportedException(
          s"$conditionName condition of UPSERT operation", cond)
      }
      if (SubqueryExpression.hasSubquery(cond)) {
        throw StarLakeErrors.subqueryNotSupportedException(
          s"$conditionName condition of UPSERT operation", cond)
      }
    }

    checkCondition(condition, "search")

    val snapshotManagement = EliminateSubqueryAliases(target) match {
      case StarLakeTableRelationV2(tbl) => tbl.snapshotManagement
      case o => throw StarLakeErrors.notAnStarLakeSourceException("Upsert", Some(o))
    }

    UpsertCommand(source, target, snapshotManagement, condition, migratedSchema)
  }
}
