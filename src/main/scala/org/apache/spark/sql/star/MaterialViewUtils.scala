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

//package org.apache.spark.sql.star

//import org.apache.spark.sql.catalyst.expressions.{Alias, AttributeReference, BinaryComparison, Cast, EqualNullSafe, EqualTo, Expression, GreaterThan, GreaterThanOrEqual, Length, LessThan, LessThanOrEqual, Literal, PredicateHelper}
//import org.apache.spark.sql.catalyst.plans.Inner
//import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, Join, LogicalPlan, Project, Sort, SubqueryAlias}
//import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
//import org.apache.spark.sql.star.catalog.StarLakeTableV2
//import org.apache.spark.sql.star.exception.StarLakeErrors
//
//import scala.collection.mutable
//
//object MaterialViewUtils extends PredicateHelper{
//
//  def parseOutputInfo(plan: LogicalPlan, constructInfo: ConstructQueryInfo): Unit ={
//    plan match {
//      case Project(projectList, child) =>
//        projectList.foreach({
//          case attr: AttributeReference =>
//            constructInfo.addOutputInfo(attr.qualifiedName, attr.sql)
//
//          case as: Alias => constructInfo.addOutputInfo(as.qualifiedName, as.child.sql)
//        })
//
//      //fast failed when parsing Aggregate with havingCondition, because it is not a real plan
//      case Aggregate(_, aggExpr, _) if aggExpr.head.isInstanceOf[Alias]
//        && aggExpr.head.asInstanceOf[Alias].name.equals("havingCondition")=>
//        throw StarLakeErrors.unsupportedLogicalPlanWhileRewriteQueryException("havingCondition")
//
//      case Aggregate(groupingExpressions, aggregateExpressions, child) =>
//        aggregateExpressions.foreach {
//          case attr: AttributeReference =>
//            constructInfo.addOutputInfo(attr.qualifiedName, attr.sql)
//
//          case as: Alias => constructInfo.addOutputInfo(as.qualifiedName, as.child.sql)
//        }
//
//
//        //fast failed when parsing unsupported query
//      case a: LogicalPlan => throw StarLakeErrors.unsupportedLogicalPlanWhileRewriteQueryException(a.toString())
//
//    }
//  }
//
//  def parseMaterialInfo(plan: LogicalPlan,
//                        constructInfo: ConstructQueryInfo,
//                        underAggregate: Boolean): Unit = {
//
//    def findTableNames(source: LogicalPlan, set: mutable.Set[String], allowJoin: Boolean): Unit = {
//      source match {
//        case _: Aggregate =>
//          throw StarLakeErrors.canNotCreateMaterialViewOrRewriteQueryException(
//          "Aggregate can't exist in Join expression and Aggregate expression")
//
//        case Join(_, _, joinType, _, _) if !(allowJoin || joinType.sql.equals(Inner.sql)) =>
//          throw StarLakeErrors.canNotCreateMaterialViewOrRewriteQueryException(
//            "Multi table join can only used with inner join")
//
//        case DataSourceV2Relation(table, _, _, _, _) if !table.isInstanceOf[StarLakeTableV2] =>
//          throw StarLakeErrors.materialViewBuildWithNonStarTableException()
//
//        case DataSourceV2Relation(table: StarLakeTableV2, _,_,_,_) => set.add(table.name())
//
//        //Recursive search table name
//        case o => o.children.foreach(findTableNames(_, set, allowJoin))
//
//      }
//    }
//
//
//    plan match {
//      case Project(projectList, child) =>
//        projectList.foreach({
//          case as: Alias => constructInfo.addColumnAsInfo("`" + as.name + "`", as.child.sql)
//          case _ =>
//        })
//        parseMaterialInfo(child, constructInfo, underAggregate)
//
//      case Filter(condition, child) =>
//        val withoutAliasCondition = condition match {
//          case Alias(c, _) => c
//          case other => other
//        }
//        val filters = splitConjunctivePredicates(withoutAliasCondition)
//        //if this filter is under Aggregate, it should match all with query
//        if(underAggregate){
//          filters.foreach {
//            case EqualTo(left, right) =>
//              constructInfo.addColumnEqualInfo(left.sql, right.sql)
//              constructInfo.setAggEqualCondition(left.sql, right.sql)
//            case other => constructInfo.setAggOtherCondition(other.sql)
//          }
//
//          parseMaterialInfo(child, constructInfo, underAggregate)
//        }else{
//          //if not under Aggregate, we can parse them to range conditions
//          filters.foreach(f => parseCondition(f, constructInfo))
//          parseMaterialInfo(child, constructInfo, underAggregate)
//        }
//
//
//      case Join(left, right, joinType, condition, hint) =>
//        val parseJoinInfo = new ParseJoinInfo()
//        val leftTables = mutable.Set[String]()
//        val rightTables = mutable.Set[String]()
//
//        //Multi table join can only used with inner join
//        val allowJoinBelow = joinType.sql.equals(Inner.sql)
//        findTableNames(left, leftTables, allowJoinBelow)
//        findTableNames(right, rightTables, allowJoinBelow)
//        parseJoinInfo.setJoinInfo(leftTables.toSet, rightTables.toSet, joinType.sql)
//
//        if(condition.isDefined){
//          if(allowJoinBelow){
//            //inner join condition can be extract to `where`
//            //sql: select * from a join b on (a.id=b.id and a.v>1) where b.v>2
//            // is semantically equivalent to
//            //sql: select * from a join b where a.id=b.id and a.v>1 and b.v>2
//            if(underAggregate){
//              //add condition to aggInfo which should match all condition when rewrite
//              splitConjunctivePredicates(condition.get).foreach {
//                case EqualTo(l, r) =>
//                  constructInfo.addColumnEqualInfo(l.sql, r.sql)
//                  constructInfo.setAggEqualCondition(l.sql, r.sql)
//                case other => constructInfo.setAggOtherCondition(other.sql)
//              }
//            }else{
//              //add condition to query level info which can match part of condition when rewrite
//              splitConjunctivePredicates(condition.get).foreach(f => parseCondition(f, constructInfo))
//            }
//          }else{
//            //add condition to join info
//            splitConjunctivePredicates(condition.get).foreach {
//              case EqualTo(left: AttributeReference, right: AttributeReference) =>
//                constructInfo.addColumnEqualInfo(left.sql, right.sql)
//                parseJoinInfo.setJoinEqualCondition(left.sql, right.sql)
//
//              case other => parseJoinInfo.setJoinOtherCondition(other.sql)
//            }
//          }
//        }
//
//
//        val joinInfo = parseJoinInfo.getJoinInfo()
//        constructInfo.addJoinInfo(joinInfo)
//        parseMaterialInfo(left, constructInfo, underAggregate)
//        parseMaterialInfo(right, constructInfo, underAggregate)
//
//
//      case Aggregate(groupingExpressions, aggregateExpressions, child) =>
//        if (underAggregate){
//          throw StarLakeErrors.canNotCreateMaterialViewOrRewriteQueryException(
//            "Multi aggregate expression is not support now")
//        }
//        //set aggregate info
//        val groupCols = groupingExpressions.map(f => f.sql)
//        val aggTables = mutable.Set[String]()
//        findTableNames(child, aggTables, true)
//        constructInfo.setAggInfo(aggTables.toSet, groupCols.toSet)
//
//        //set alias info
//        aggregateExpressions.foreach({
//          case as: Alias => constructInfo.addColumnAsInfo("`" + as.name + "`", as.child.sql)
//          case _ =>
//        })
//
//        parseMaterialInfo(child, constructInfo, true)
//
//
//
//      case SubqueryAlias(ident, child) =>
//        val prefix = ident.toString() + ".`"
//        child match {
//          case Project(projectList, _) =>
//            projectList.foreach({
//              case attr: AttributeReference =>
//                constructInfo.addColumnAsInfo(prefix + attr.name + "`", attr.sql)
//
//              case as: Alias =>
//                constructInfo.addColumnAsInfo(prefix + as.name + "`", as.child.sql)
//            })
//
//          case SubqueryAlias(identifier, _) =>
//            constructInfo.addTableInfo(ident.toString(), identifier.toString())
//
//          case DataSourceV2Relation(table: StarLakeTableV2, _,_,_,_) =>
//            constructInfo.addTableInfo(ident.toString(), table.name())
//
//          //
//          case a: LogicalPlan => throw StarLakeErrors.canNotCreateMaterialViewOrRewriteQueryException(
//            "unsupported child plan when parse SubqueryAlias")
//
//        }
//
//        parseMaterialInfo(child, constructInfo, underAggregate)
//
//      case DataSourceV2Relation(table: StarLakeTableV2, _, _, _, _) =>
//        if(table.snapshotManagement.getTableInfoOnly.is_material_view){
//          throw StarLakeErrors.canNotCreateMaterialViewOrRewriteQueryException(
//            "A material view can't be used to create or rewrite another material view")
//        }
//        constructInfo.addTableInfo(table.name(), table.name())
//
//      case DataSourceV2Relation(table, _, _, _, _) if !table.isInstanceOf[StarLakeTableV2] =>
//        throw StarLakeErrors.materialViewBuildWithNonStarTableException()
//
//
//      case lp: LogicalPlan if lp.children.nonEmpty =>
//        parseMaterialInfo(lp, constructInfo, underAggregate)
//
//      case _ =>
//
//    }
//  }
//
//
//  //parse conditions which can
//  private def parseCondition(condition: Expression, constructInfo: ConstructQueryInfo): Unit = {
//    condition match {
//
//      case e @ EqualTo(left, right) =>
//        if (right.isInstanceOf[Literal]){
//          constructInfo.addConditionEqualInfo(left.sql, right.asInstanceOf[Literal].value.toString)
//        }else if(left.isInstanceOf[Literal]){
//          constructInfo.addConditionEqualInfo(right.sql, left.asInstanceOf[Literal].value.toString)
//        }else{
//          constructInfo.addColumnEqualInfo(left.sql, right.sql)
//          //sort to match equal condition, like (t1.a=t2.a) compare to (t2.a=t1.a)
//          if(left.sql.compareTo(right.sql) <= 0){
//            constructInfo.addOtherInfo(e.sql)
//          }else{
//            constructInfo.addOtherInfo(e.copy(left = right, right = left).sql)
//          }
//        }
//
//
//      case e @ EqualNullSafe(left, right) =>
//        if (right.isInstanceOf[Literal]){
//          constructInfo.addConditionEqualInfo(left.sql, right.asInstanceOf[Literal].value.toString)
//        }else if(left.isInstanceOf[Literal]){
//          constructInfo.addConditionEqualInfo(right.sql, left.asInstanceOf[Literal].value.toString)
//        }else{
//          constructInfo.addColumnEqualInfo(left.sql, right.sql)
//          //sort to match equal condition, like (t1.a=t2.a) compare to (t2.a=t1.a)
//          if(left.sql.compareTo(right.sql) <= 0){
//            constructInfo.addOtherInfo(e.sql)
//          }else{
//            constructInfo.addOtherInfo(e.copy(left = right, right = left).sql)
//          }
//        }
////        constructInfo.addColumnEqualInfo(left.sql, right.sql)
////        constructInfo.addOtherInfo(e.sql)
//
//      case GreaterThan(left, right: Literal) =>
//        constructInfo.addRangeInfo(left.dataType, left.sql, right.value, "GreaterThan")
//
//      case GreaterThan(left: Literal, right) =>
//        constructInfo.addRangeInfo(right.dataType, right.sql, left.value, "LessThan")
//
//      case GreaterThanOrEqual(left, right: Literal) =>
//        constructInfo.addRangeInfo(left.dataType, left.sql, right.value, "GreaterThanOrEqual")
//
//      case GreaterThanOrEqual(left: Literal, right) =>
//        constructInfo.addRangeInfo(right.dataType, right.sql, left.value, "LessThanOrEqual")
//
//      case LessThan(left, right: Literal) =>
//        constructInfo.addRangeInfo(left.dataType, left.sql, right.value, "LessThan")
//
//      case LessThan(left: Literal, right) =>
//        constructInfo.addRangeInfo(right.dataType, right.sql, left.value, "GreaterThan")
//
//      case LessThanOrEqual(left, right: Literal) =>
//        constructInfo.addRangeInfo(left.dataType, left.sql, right.value, "LessThanOrEqual")
//
//      case LessThanOrEqual(left: Literal, right) =>
//        constructInfo.addRangeInfo(right.dataType, right.sql, left.value, "GreaterThanOrEqual")
//
//        //todo: make a better matching on or and other complex condition
//      case other => constructInfo.addOtherInfo(other.sql)
//
//
//
//    }
//  }
//
//
//
//
//
//}
