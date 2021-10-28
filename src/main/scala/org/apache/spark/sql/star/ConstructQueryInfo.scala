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

import java.util

import com.alibaba.fastjson.JSON
import com.engineplus.star.meta.MetaUtils
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.star.exception.StarLakeErrors
import org.apache.spark.sql.types.{BinaryType, BooleanType, ByteType, CalendarIntervalType, CharType, DataType, DateType, Decimal, DecimalType, DoubleType, FloatType, IntegerType, LongType, NullType, ShortType, StringType, TimestampType, VarcharType}
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.sql.star.ConstructQueryInfo._

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConverters._

class ConstructQueryInfo {
  private var firstConstruct: Boolean = true

  private val outputInfo: mutable.Map[String, String] = mutable.Map[String, String]()
//  private val equalInfo: mutable.Map[String, mutable.Set[String]] = mutable.Map[String, mutable.Set[String]]()

  private val equalInfo: ArrayBuffer[(String, String)] = new ArrayBuffer[(String, String)]()

  //  private val conditionInfo: ArrayBuffer[String] = new ArrayBuffer[String]()
  //  private val joinInfo: ArrayBuffer[String] = new ArrayBuffer[String]()
  //  private val aggregateInfo: ArrayBuffer[String] = new ArrayBuffer[String]()
  private val tableInfo: mutable.Map[String, String] = mutable.Map[String, String]()
  private val columnAsInfo: mutable.Map[String, String] = mutable.Map[String, String]()
  private val rangeInfo: mutable.Map[String, RangeDetail] = mutable.Map[String, RangeDetail]()
  private val otherInfo: ArrayBuffer[String] = new ArrayBuffer[String]()

  //  private var joinType: Option[String] = _
  //  private var joinEqualConditions: ArrayBuffer[(String, String)] = _
  //  private var joinOtherConditions: ArrayBuffer[String] = new ArrayBuffer[String]()
  private val joinInfo: ArrayBuffer[JoinInfo] = new ArrayBuffer[JoinInfo]()
  private val aggregateInfo: ParseAggregateInfo = new ParseAggregateInfo()


  def addJoinInfo(info: JoinInfo): Unit = {
    joinInfo += info
  }

  def addOutputInfo(name: String, referenceName: String): Unit = {
    val re = outputInfo.put(name, referenceName)
    assert(re.isEmpty, s"outputInfo map already has key($name), " +
      s"exists value(${re.get}), new value($referenceName)")
  }

  //  def addAggregateInfo(name: String): Unit = {
  //    aggregateInfo += name
  //  }


  def addTableInfo(name: String, referenceName: String): Unit = {
    val re = tableInfo.put(name, referenceName)
    assert(re.isEmpty, s"tableInfo map already has key($name), " +
      s"exists value(${re.get}), new value($referenceName)")
  }

  def addColumnAsInfo(name: String, referenceName: String): Unit = {
    if (!referenceName.replace("`", "").equals(name)) {
      val re = columnAsInfo.put(name, referenceName)
      assert(re.isEmpty, s"columnAsInfo map already has key($name), " +
        s"exists value(${re.get}), new value($referenceName)")
    }
  }


  def addOtherInfo(info: String): Unit = {
    otherInfo += info
  }

//  def addEqualInfo(left: String, right: String): Unit = {
//    //union exist info
//    val unionSet =
//      if (equalInfo.contains(left)) {
//        if (equalInfo.contains(right)) {
//          equalInfo(left) ++ equalInfo(right)
//        } else {
//          equalInfo(left) += right
//        }
//      } else if (equalInfo.contains(right)) {
//        equalInfo(right) += left
//      } else {
//        val newSet = new mutable.HashSet[String]()
//        newSet += left
//        newSet += right
//      }
//
//    //update all related elements
//    unionSet.foreach(f => equalInfo.put(f, unionSet))
//  }

  def addEqualInfo(left: String, right: String): Unit = {
    equalInfo += ((left, right))
  }



  def addRangeInfo(dataType: DataType, colName: String, limit: Any, rangeType: String): Unit = {
    val detail = rangeInfo.getOrElse(
      colName,
      RangeDetail(dataType, null, null, true, true)
    )

    val newDetail = rangeType match {
      case "GreaterThan" =>
        if (detail.lower == null) {
          detail.copy(lower = limit, includeLower = false)
        } else {
          val re = RangeDetail.compareRange(limit, detail.lower, dataType)
          if (re >= 0) {
            detail.copy(lower = limit, includeLower = false)
          } else {
            detail
          }
        }

      case "GreaterThanOrEqual" =>
        if (detail.lower == null) {
          detail.copy(lower = limit, includeLower = true)
        } else {
          val re = RangeDetail.compareRange(limit, detail.lower, dataType)
          if (re > 0) {
            detail.copy(lower = limit, includeLower = true)
          } else if (re == 0) {
            detail.copy(lower = limit)
          } else {
            detail
          }
        }

      case "LessThan" =>
        if (detail.upper == null) {
          detail.copy(upper = limit, includeUpper = false)
        } else {
          val re = RangeDetail.compareRange(limit, detail.upper, dataType)
          if (re <= 0) {
            detail.copy(upper = limit, includeUpper = false)
          } else {
            detail
          }
        }

      case "LessThanOrEqual" =>
        if (detail.upper == null) {
          detail.copy(upper = limit, includeUpper = true)
        } else {
          val re = RangeDetail.compareRange(limit, detail.lower, dataType)
          if (re < 0) {
            detail.copy(upper = limit, includeUpper = true)
          } else if (re == 0) {
            detail.copy(upper = limit)
          } else {
            detail
          }
        }
    }

    rangeInfo.put(colName, newDetail)

  }

  def setAggEqualCondition(left: String, right: String): Unit = {
    aggregateInfo.setAggEqualCondition(left, right)
  }

  def setAggOtherCondition(cond: String): Unit = {
    aggregateInfo.setAggOtherCondition(cond)
  }

  def setAggInfo(tables: Set[String], cols: Set[String]): Unit = {
    aggregateInfo.setAggInfo(tables, cols)
  }


  def buildQueryInfo(): QueryInfo = {
    assert(firstConstruct, "It has been built before, you can't build query info more than once")
    firstConstruct = false

    //matching final star table
    val tables = tableInfo.keys
      .filter(f => !f.startsWith("star.`"))
      .map(key => {
        var flag = true
        var value = tableInfo(key)
        while (flag) {
          value = tableInfo(value)
          if (value.startsWith("star.`")) {
            flag = false
          }
        }
        (key, value)
      }).toMap

    //replace table to final star table
    val columnAsInfoTmp = columnAsInfo.map(m => {
      m._1 -> replaceByTableInfo(tables, m._2)
    }).toMap

    //replace column as condition to final star table column
    val columnAsInfoNew = columnAsInfoTmp.map(m => {
      var flag = true
      var value = m._2
      while(flag){
        if(columnAsInfoTmp.contains(value)){
          value = columnAsInfoTmp(value)
        }else{
          flag = false
        }
      }
      m._1 -> value
    })

    //build a map to replace condition string
//    val asInfo = columnAsInfoNew.filter(f => f._1.contains(".`"))

    val outputInfoNew = outputInfo.map(m => {
      //replace temp table to final star table
      val formatName = replaceByTableInfo(tables, m._2)

      val finalName = if (columnAsInfoNew.contains(formatName)){
        //replace alias fields to final star table fields
        columnAsInfoNew(formatName)
      }else{
        formatName
      }

      m._1 -> finalName
    }).toMap



    val equalInfoTmp = equalInfo.map(m => {
      val key = replaceByColumnAsInfo(columnAsInfoNew, replaceByTableInfo(tables, m._1))
      val value = replaceByColumnAsInfo(columnAsInfoNew, replaceByTableInfo(tables, m._2))
      (key, value)
    })

    val equalInfoNew: ArrayBuffer[mutable.Set[String]] = new ArrayBuffer[mutable.Set[String]]()

    for (info <- equalInfoTmp){
      val find = equalInfoNew.find(f => f.contains(info._1) || f.contains(info._2))
      if (find.isDefined){
        find.get.add(info._1)
        find.get.add(info._2)
      }else{
        val set = mutable.Set[String]()
        set.add(info._1)
        set.add(info._2)
        equalInfoNew += set
      }
    }


    val rangeInfoNew = rangeInfo.map(m => {
      replaceByColumnAsInfo(columnAsInfoNew, replaceByTableInfo(tables, m._1)) -> m._2
    }).toMap


    val joinInfoNew = if (joinInfo.isEmpty) {
      JoinInfo.buildEmptyInfo()
    } else if (joinInfo.head.joinType.equals(Inner.sql)) {
      assert(joinInfo.forall(f => Inner.sql.equals(f.joinType)), "Multi table join can only used with inner join")
      assert(joinInfo.forall(f => f.otherCondition.isEmpty && f.equalCondition.isEmpty),
        "Inner join condition should be extract with where condition")

      //transform tables to final star table
      val left = joinInfo
        .flatMap(m => m.leftTables ++ m.rightTables)
        .map(m => {
          if (m.startsWith("star.`")) {
            m
          } else {
            tables(m)
          }
        })
        .toSet
      JoinInfo(
        left,
        Set.empty[String],
        Inner.sql,
        Set.empty[(String, String)],
        Set.empty[String])
    } else {
      assert(joinInfo.length == 1, "Multi table join can only used with inner join")

      //transform tables to final star table
      val left = joinInfo.head.leftTables.map(m => {
        if (m.startsWith("star.`")) {
          m
        } else {
          tables(m)
        }
      })
      val right = joinInfo.head.rightTables.map(m => {
        if (m.startsWith("star.`")) {
          m
        } else {
          tables(m)
        }
      })
      val equalCondition = joinInfo.head.equalCondition.map(cond => {
        val key = replaceByColumnAsInfo(columnAsInfoNew, replaceByTableInfo(tables, cond._1))
        val value = replaceByColumnAsInfo(columnAsInfoNew, replaceByTableInfo(tables, cond._2))
        (key, value)
      })
      val otherCondition = joinInfo.head.otherCondition.map(m =>
        replaceByColumnAsInfo(columnAsInfoNew, replaceByTableInfo(tables, m)))

      JoinInfo(
        left,
        right,
        joinInfo.head.joinType,
        equalCondition,
        otherCondition
      )

    }



    val aggregateInfoNew = aggregateInfo.getFinalAggregateInfo(tables)

    val otherInfoNew = otherInfo.map(m => replaceByColumnAsInfo(columnAsInfoNew, replaceByTableInfo(tables, m)))

    QueryInfo(
      outputInfoNew,
      equalInfoNew.map(m => m.toSet),
      columnAsInfoNew,
      rangeInfoNew,
      joinInfoNew,
      aggregateInfoNew,
      otherInfoNew,
      tables)

  }


}

object ConstructQueryInfo {

  //replace the prefix of fields to final star table name
//  def replaceToFinalTable(allTables: Map[String, String], str: String): String = {
//    var result = str
//    allTables.foreach(t => {
//      result = result.replace(s"${t._1}.`", s"${t._2}.`")
//    })
//    result
//  }

  def replaceByTableInfo(allTables: Map[String, String], str: String): String = {
    var result = str
    allTables.foreach(t => {
      result = result.replace(s"${t._1}.`", s"${t._2}.`")
    })
    result
  }

  def replaceByColumnAsInfo(asInfo: Map[String, String], str: String): String = {
    var result = str
    asInfo.foreach(t => {
      result = result.replace(t._1, t._2)
      //eg: use Map(`key` -> star.`t1`) to replace string "concat_ws(',',`key`,a.`key`)"
      //result will be "concat_ws(',',star.`t1`,a.star.`t1`)", it is not expect result,
      //use another Map(.star.`t1` -> .`key`) to get the expect result "concat_ws(',',star.`t1`,a.`key`)"
      result = result.replace(s".${t._2}", s".${t._1}")
    })
    result
  }

//  //replace alias fields to final star table fields
//  def replaceWithColumnAsInfo(columnAsInfo: Map[String, String], str: String): String = {
//    if (str.startsWith("star.`")){
//      str
//    }else{
//      val result = columnAsInfo(str.replace("`", ""))
//      replaceWithColumnAsInfo(columnAsInfo, result)
//    }
//  }

  def buildJson(info: QueryInfo): String = {
    //build json
    val jsonMap = new util.HashMap[String, String]()

    val outputMap = new util.HashMap[String, String]()
    info.outputInfo.foreach(info => {
      outputMap.put(info._1, info._2)
    })
    jsonMap.put("outputInfo", JSON.toJSON(outputMap).toString)

//    val equalMap = new util.HashMap[String, String]()
//    info.equalInfo.foreach(info => {
//      equalMap.put(info._1, info._2.mkString(MetaUtils.STAR_LAKE_SEP_01))
//    })
//    jsonMap.put("equalInfo", JSON.toJSON(equalMap).toString)


    val equalInfoString = info.equalInfo
      .map(m => m.mkString(MetaUtils.STAR_LAKE_SEP_01))
      .mkString(MetaUtils.STAR_LAKE_SEP_02)
    jsonMap.put("equalInfo", equalInfoString)

    val columnAsMap = new util.HashMap[String, String]()
    info.columnAsInfo.foreach(info => {
      columnAsMap.put(info._1, info._2)
    })
    jsonMap.put("columnAsInfo", JSON.toJSON(columnAsMap).toString)

    val rangeMap = new util.HashMap[String, String]()
    info.rangeInfo.foreach(info => {
      rangeMap.put(info._1, info._2.toString)
    })
    jsonMap.put("rangeInfo", JSON.toJSON(rangeMap).toString)

    jsonMap.put("joinInfo", info.joinInfo.toString)

    jsonMap.put("aggregateInfo", info.aggregateInfo.toString)

    //    val otherMap = new util.HashMap[String, String]()
    jsonMap.put("otherInfo", info.otherInfo.mkString(MetaUtils.STAR_LAKE_SEP_01))


    //trans all info to json and replace table to final star table
    var json = JSON.toJSON(jsonMap).toString
    json = json.replace("'", MetaUtils.STAR_META_QUOTE)

    json
  }


  //build info from json string which got from meta data
  def buildInfo(jsonString: String): QueryInfo = {
    val jsonObj = JSON.parseObject(jsonString.replace(MetaUtils.STAR_META_QUOTE, "'"))

    val outputJson = JSON.parseObject(jsonObj.getString("outputInfo"))
    val outputInfo = outputJson.getInnerMap.asScala.map(m => m._1 -> m._2.toString).toMap

//    val equalJson = JSON.parseObject(jsonObj.getString("equalInfo"))
//    val equalInfo = equalJson.getInnerMap.asScala.map(m => {
//      m._1 -> m._2.toString.split(MetaUtils.STAR_LAKE_SEP_01).toSet
//    }).toMap

    val equalString = jsonObj.getString("equalInfo")
    val equalInfo = if(equalString.equals("")){
      Seq.empty[Set[String]]
    }else{
      equalString
        .split(MetaUtils.STAR_LAKE_SEP_02)
        .map(m => m.split(MetaUtils.STAR_LAKE_SEP_01).toSet)
        .toSeq
    }

    val columnAsJson = JSON.parseObject(jsonObj.getString("columnAsInfo"))
    val columnAsInfo = columnAsJson.getInnerMap.asScala.map(m => m._1 -> m._2.toString).toMap

    val rangeJson = JSON.parseObject(jsonObj.getString("rangeInfo"))
    val rangeInfo = rangeJson.getInnerMap.asScala.map(m => {
      val detail = RangeDetail.build(m._2.toString)
      m._1 -> detail
    }).toMap

    val joinJson = jsonObj.getString("joinInfo")
    val joinInfo = JoinInfo.build(joinJson)

    val aggregateJson = jsonObj.getString("aggregateInfo")
    val aggregateInfo = AggregateInfo.build(aggregateJson)

    val otherInfoString = jsonObj.getString("otherInfo")
    val otherInfo = if (otherInfoString.equals("")) {
      Seq.empty[String]
    } else {
      otherInfoString.split(MetaUtils.STAR_LAKE_SEP_01).toSeq
    }

    val tableInfo = Map.empty[String, String]
    QueryInfo(
      outputInfo,
      equalInfo,
      columnAsInfo,
      rangeInfo,
      joinInfo,
      aggregateInfo,
      otherInfo,
      tableInfo)


  }


}


//all query/table info
case class QueryInfo(outputInfo: Map[String, String],
                     equalInfo: Seq[Set[String]],
                     columnAsInfo: Map[String, String],
                     rangeInfo: Map[String, RangeDetail],
                     joinInfo: JoinInfo,
                     aggregateInfo: AggregateInfo,
                     otherInfo: Seq[String],
                     tableInfo: Map[String, String])


//range detail info of query/table
case class RangeDetail(dataType: DataType,
                       lower: Any,
                       upper: Any,
                       includeLower: Boolean,
                       includeUpper: Boolean) {
  override def toString: String = {
    dataType.toString + "," + getString(lower) + "," + getString(upper) + "," +
      includeLower.toString + "," + includeUpper.toString
  }

  private def getString(str: Any): String = {
    if (str == null) {
      "_STAR_META_NULL_"
    } else {
      str.toString
    }
  }
}

object RangeDetail {

  def matchEqual(a: RangeDetail, b: RangeDetail): Boolean = {
    val lowerMatch = if (a.lower == null && b.lower == null) {
      true
    } else if (a.lower != null && b.lower != null) {
      if (a.includeLower == b.includeLower && transAndCompareRange(a.lower, b.lower, a.dataType) == 0) {
        true
      } else {
        false
      }
    } else {
      false
    }

    if (lowerMatch) {
      if (a.upper == null && b.upper == null) {
        true
      } else if (a.upper != null && b.upper != null) {
        if (a.includeUpper == b.includeUpper && transAndCompareRange(a.upper, b.upper, a.dataType) == 0) {
          true
        } else {
          false
        }
      } else {
        false
      }
    } else {
      false
    }
  }

  def compareRange(left: Any, right: Any, dataType: DataType): Int = {
    dataType match {
      case BooleanType => left.asInstanceOf[Boolean].compareTo(right.asInstanceOf[Boolean])
      case ByteType => left.asInstanceOf[Byte].compareTo(right.asInstanceOf[Byte])
      case ShortType => left.asInstanceOf[Short].compareTo(right.asInstanceOf[Short])
      case IntegerType | DateType => left.asInstanceOf[Int].compareTo(right.asInstanceOf[Int])
      case LongType | TimestampType => left.asInstanceOf[Long].compareTo(right.asInstanceOf[Long])
      case FloatType => left.asInstanceOf[Float].compareTo(right.asInstanceOf[Float])
      case DoubleType => left.asInstanceOf[Double].compareTo(right.asInstanceOf[Double])
      case StringType => left.asInstanceOf[UTF8String].compareTo(right.asInstanceOf[UTF8String])
      case _ => throw StarLakeErrors.unsupportedDataTypeInMaterialRewriteQueryException(dataType)
    }
  }


  def transAndCompareRange(left: Any, right: Any, dataType: DataType): Int = {
    dataType match {
      case BooleanType => left.toString.toBoolean.compareTo(right.toString.toBoolean)
      case ByteType => left.toString.toByte.compareTo(right.toString.toByte)
      case ShortType => left.toString.toShort.compareTo(right.toString.toShort)
      case IntegerType | DateType => left.toString.toInt.compareTo(right.toString.toInt)
      case LongType | TimestampType => left.toString.toLong.compareTo(right.toString.toLong)
      case FloatType => left.toString.toFloat.compareTo(right.toString.toFloat)
      case DoubleType => left.toString.toDouble.compareTo(right.toString.toDouble)
      case StringType => UTF8String.fromString(left.toString).compareTo(UTF8String.fromString(right.toString))
      case _ => throw StarLakeErrors.unsupportedDataTypeInMaterialRewriteQueryException(dataType)
    }
  }


  private def getValue(str: String): Any = {
    if (str.equals("_STAR_META_NULL_")) {
      null
    } else {
      str
    }
  }

  private val FIXED_DECIMAL = """decimal\(\s*(\d+)\s*,\s*(\-?\d+)\s*\)""".r
  private val CHAR_TYPE = """char\(\s*(\d+)\s*\)""".r
  private val VARCHAR_TYPE = """varchar\(\s*(\d+)\s*\)""".r

  private val otherTypes = {
    Seq(
      ("NullType", NullType),
      ("DateType", DateType),
      ("TimestampType", TimestampType),
      ("BinaryType", BinaryType),
      ("IntegerType", IntegerType),
      ("BooleanType", BooleanType),
      ("LongType", LongType),
      ("DoubleType", DoubleType),
      ("FloatType", FloatType),
      ("ShortType", ShortType),
      ("ByteType", ByteType),
      ("StringType", StringType),
      ("CalendarIntervalType", CalendarIntervalType))
      .toMap
  }

  /** Given the string representation of a type, return its DataType */
  private def nameToType(name: String): DataType = {
    name match {
      case "decimal" => DecimalType.USER_DEFAULT
      case FIXED_DECIMAL(precision, scale) => DecimalType(precision.toInt, scale.toInt)
      case CHAR_TYPE(length) => CharType(length.toInt)
      case VARCHAR_TYPE(length) => VarcharType(length.toInt)
      case other => otherTypes.getOrElse(
        other,
        throw new IllegalArgumentException(
          s"Failed to convert the JSON string '$name' to a data type."))
    }
  }

  def build(str: String): RangeDetail = {
    val split = str.split(",")
    RangeDetail(
      nameToType(split(0)),
      getValue(split(1)),
      getValue(split(2)),
      split(3).toBoolean,
      split(4).toBoolean)
  }
}


//class ParseJoinInfo {
//  private var leftTable: String = _
//  private var rightTable: String = _
//
//  private var joinType: String = _
//
//  private val joinEqualConditions: mutable.Map[String, String]  = mutable.Map[String, String]()
//  private val joinOtherConditions: ArrayBuffer[String] = new ArrayBuffer[String]()
//
//  def setJoinEqualCondition(left: String, right: String): Unit = {
//    if (left.compareTo(right) <= 0){
//      joinEqualConditions.put(left, right)
//    }else{
//      joinEqualConditions.put(right, left)
//    }
//  }
//
//  def setJoinOtherCondition(cond: String): Unit ={
//    joinOtherConditions += cond
//  }
//
//  def setJoinInfo(left: String, right: String, jt: String): Unit ={
//    leftTable = left
//    rightTable = right
//    joinType = jt
//  }
//
//  def getJoinInfo(): JoinInfo ={
//    JoinInfo(leftTable,
//      rightTable,
//      joinType,
//      joinEqualConditions.toMap,
//      joinOtherConditions)
//  }
//
//
//}
//
//case class JoinInfo(leftTable: String,
//                    rightTable: String,
//                    joinType: String,
//                    equalCondition: Map[String, String],
//                    otherCondition: Seq[String]){
//  override def toString: String = {
//    val equal = equalCondition.map(m => m._1 + "\t" + m._2).mkString(";")
//    val other = otherCondition.mkString(";")
//
//    Seq(leftTable, rightTable, joinType, equal, other)
//      .mkString("\001")
//  }
//}

//object JoinInfo {
//  def build(str: String): JoinInfo = {
//    val split = str.split("\001", -1)
//    assert(split.length == 5)
//    val equal = split(3).split(";", -1).map(m => {
//      val arr = m.split("\t")
//      assert(arr.length == 2)
//      arr(0) -> arr(1)
//    }).toMap
//    val other = if(split(4).equals("")){
//      Seq.empty[String]
//    }else{
//      split(4).split(";").toSeq
//    }
//
//    JoinInfo(split(0), split(1), split(2), equal, other)
//  }
//}


///////////////////////////////////////////////////////
///////////////////////////////////////////////////////


class ParseJoinInfo {
  private var leftTables: Set[String] = _
  private var rightTables: Set[String] = _

  private var joinType: String = _

  private val joinEqualConditions: mutable.Set[(String, String)] = mutable.Set[(String, String)]()
  private val joinOtherConditions: ArrayBuffer[String] = new ArrayBuffer[String]()

  def setJoinEqualCondition(left: String, right: String): Unit = {
    if (left.compareTo(right) <= 0) {
      joinEqualConditions.add(left, right)
    } else {
      joinEqualConditions.add(right, left)
    }
  }

  def setJoinOtherCondition(cond: String): Unit = {
    joinOtherConditions += cond
  }

  def setJoinInfo(left: Set[String], right: Set[String], jt: String): Unit = {
    leftTables = left
    rightTables = right
    joinType = jt
  }

  def getJoinInfo(): JoinInfo = {
    JoinInfo(
      leftTables,
      rightTables,
      joinType,
      joinEqualConditions.toSet,
      joinOtherConditions.toSet)
  }


}

case class JoinInfo(leftTables: Set[String],
                    rightTables: Set[String],
                    joinType: String,
                    equalCondition: Set[(String, String)],
                    otherCondition: Set[String]) {
  override def toString: String = {
    val equal = equalCondition
      .map(m => m._1 + MetaUtils.STAR_LAKE_SEP_01 + m._2)
      .mkString(MetaUtils.STAR_LAKE_SEP_02)
    val other = otherCondition.mkString(MetaUtils.STAR_LAKE_SEP_02)

    Seq(leftTables.mkString(MetaUtils.STAR_LAKE_SEP_02),
      rightTables.mkString(MetaUtils.STAR_LAKE_SEP_02),
      joinType,
      equal,
      other)
      .mkString(MetaUtils.STAR_LAKE_SEP_03)
  }
}

object JoinInfo {
  def buildEmptyInfo(): JoinInfo = {
    JoinInfo(
      Set.empty[String],
      Set.empty[String],
      "",
      Set.empty[(String, String)],
      Set.empty[String])
  }


  def build(str: String): JoinInfo = {
    val split = str.split(MetaUtils.STAR_LAKE_SEP_03, -1)
    assert(split.length == 5)

    val leftTables = if (split(0).equals("")) {
      Set.empty[String]
    } else {
      split(0).split(MetaUtils.STAR_LAKE_SEP_02).toSet
    }
    val rightTables = if (split(1).equals("")) {
      Set.empty[String]
    } else {
      split(1).split(MetaUtils.STAR_LAKE_SEP_02).toSet
    }
    val equal = if (split(3).equals("")) {
      Set.empty[(String, String)]
    } else {
      split(3).split(MetaUtils.STAR_LAKE_SEP_02, -1).map(m => {
        val arr = m.split(MetaUtils.STAR_LAKE_SEP_01)
        assert(arr.length == 2)
        (arr(0), arr(1))
      }).toSet
    }
    val other = if (split(4).equals("")) {
      Set.empty[String]
    } else {
      split(4).split(MetaUtils.STAR_LAKE_SEP_02).toSet
    }

    JoinInfo(
      leftTables,
      rightTables,
      split(2),
      equal,
      other)
  }
}


class ParseAggregateInfo {
  private var aggTables: Set[String] = Set.empty[String]
  private var aggColumns: Set[String] = Set.empty[String]

  private val aggEqualConditions: mutable.Set[(String, String)] = mutable.Set[(String, String)]()
  private val aggOtherConditions: ArrayBuffer[String] = new ArrayBuffer[String]()

  def setAggEqualCondition(left: String, right: String): Unit = {
    if (left.compareTo(right) <= 0) {
      aggEqualConditions.add(left, right)
    } else {
      aggEqualConditions.add(right, left)
    }
  }

  def setAggOtherCondition(cond: String): Unit = {
    aggOtherConditions += cond
  }

  def setAggInfo(tables: Set[String], cols: Set[String]): Unit = {
    aggTables = tables
    aggColumns = cols
  }

  def getAggregateInfo(): AggregateInfo = {
    AggregateInfo(
      aggTables,
      aggColumns,
      aggEqualConditions.toSet,
      aggOtherConditions.toSet)
  }

  def getFinalAggregateInfo(tables: Map[String, String]): AggregateInfo = {
    AggregateInfo(
      aggTables.map(replaceByTableInfo(tables, _)),
      aggColumns.map(replaceByTableInfo(tables, _)),
      aggEqualConditions.map(m => (replaceByTableInfo(tables, m._1), replaceByTableInfo(tables, m._2))).toSet,
      aggOtherConditions.map(replaceByTableInfo(tables, _)).toSet)
  }


}

case class AggregateInfo(tables: Set[String],
                         columns: Set[String],
                         equalCondition: Set[(String, String)],
                         otherCondition: Set[String]) {
  override def toString: String = {
    val equal = equalCondition
      .map(m => m._1 + MetaUtils.STAR_LAKE_SEP_01 + m._2)
      .mkString(MetaUtils.STAR_LAKE_SEP_02)
    val other = otherCondition.mkString(MetaUtils.STAR_LAKE_SEP_02)

    Seq(tables.mkString(MetaUtils.STAR_LAKE_SEP_02),
      columns.mkString(MetaUtils.STAR_LAKE_SEP_02),
      equal,
      other)
      .mkString(MetaUtils.STAR_LAKE_SEP_03)
  }
}


object AggregateInfo {
  def build(str: String): AggregateInfo = {
    val split = str.split(MetaUtils.STAR_LAKE_SEP_03, -1)
    assert(split.length == 4)
    val equal = if (split(2).equals("")) {
      Set.empty[(String, String)]
    } else {
      split(2).split(MetaUtils.STAR_LAKE_SEP_02, -1).map(m => {
        val arr = m.split(MetaUtils.STAR_LAKE_SEP_01)
        assert(arr.length == 2)
        (arr(0), arr(1))
      }).toSet
    }
    val other = if (split(3).equals("")) {
      Set.empty[String]
    } else {
      split(3).split(MetaUtils.STAR_LAKE_SEP_02).toSet
    }

    AggregateInfo(
      split(0).split(MetaUtils.STAR_LAKE_SEP_02).toSet,
      split(1).split(MetaUtils.STAR_LAKE_SEP_02).toSet,
      equal,
      other)
  }
}