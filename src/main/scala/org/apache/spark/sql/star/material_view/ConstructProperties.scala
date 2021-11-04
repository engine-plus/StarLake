package org.apache.spark.sql.star.material_view

import org.apache.spark.sql.types.DataType

/**
  * A
  */
trait ConstructProperties {

  def addRangeInfo(dataType: DataType, colName: String, limit: Any, rangeType: String): Unit
  def addConditionEqualInfo(left: String, right: String): Unit
  def addColumnEqualInfo(left: String, right: String): Unit
  def addConditionOrInfo(orInfo: OrInfo): Unit
  def addOtherInfo(info: String): Unit

}
