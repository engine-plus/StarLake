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

package org.apache.spark.sql.execution.datasources.v2.merge.parquet.row

import java.util.Comparator

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.execution.datasources.v2.merge.MergePartitionedFile
import org.apache.spark.sql.types._

import scala.collection.mutable

/**
  * Created by luozhenglin 
  * on 2021/2/4 6:05 PM
  */
class MergeParquetFileParitionByRowReader[T](fileIterators: Seq[(MergePartitionedFile, PartitionReader[InternalRow])])
  extends PartitionReader[InternalRow] with Logging {

  type heapType = (Long, InternalRow)
  val rowMinHeap = new mutable.PriorityQueue[heapType]()(
    (x: heapType, y: heapType) => comparatorT.compare(y, x))

  //mergefile（version） ->  seq(field_index,structType)  seq 按序排列
  var indexKeyMap: Map[Long, Array[(Int, DataType)]] = _

  private val comparatorT = new Comparator[heapType] {
    override def compare(x: (Long, InternalRow),
                         y: (Long, InternalRow)): Int = {
      for (i <- indexKeyMap(x._1).indices) {
        val inputX = x._2
        val inputY = y._2
        val ordinalX = indexKeyMap(x._1)(i)._1
        val ordinalY = indexKeyMap(y._1)(i)._1
        val comparV =
          indexKeyMap(x._1)(i)._2 match {
            case StringType => inputX.getUTF8String(ordinalX).compareTo(inputY.getUTF8String(ordinalY))
            case IntegerType | DateType => inputX.getInt(ordinalX) compareTo (inputY getInt ordinalY)
            case BooleanType => inputX.getBoolean(ordinalX) compareTo inputY.getBoolean(ordinalY)
            case ByteType => inputX.getByte(ordinalX).compareTo(inputY.getByte(ordinalY))
            case ShortType => inputX.getShort(ordinalX).compareTo(inputY.getShort(ordinalY))
            case LongType | TimestampType => inputX.getLong(ordinalX).compareTo(inputY.getLong(ordinalY))
            case FloatType => inputX.getFloat(ordinalX).compareTo(inputY.getFloat(ordinalY))
            case DoubleType => inputX.getDouble(ordinalX).compareTo(inputY.getDouble(ordinalY))
            case t: DecimalType => inputX.getDecimal(ordinalX, t.precision, t.scale).compareTo(inputY.getDecimal(ordinalY, t.precision, t.scale))
            case _ => throw new RuntimeException("Unsupported data type for merge,type is " + indexKeyMap(x._1)(i)._2.getClass.getTypeName)
          }
        if (comparV != 0) {
          return comparV
        }
      }
      x._1.compareTo(y._1)
    }
  }

  var mergeCurrentRow: InternalRow = null

  override def next(): Boolean = {
    if (fileIterators.isEmpty) return false

    //check nextRow has data and check priority Queue
    rowMinHeap.isEmpty match {
      case true =>
        mergeCurrentRow match {
          //first:  1.init required index. 2.enter priority Queue. 3.merge logic
          case null =>

            return true
          //last: not have more record after merge end  so return false
          case _ => return false
        }
      //merge logic: get all same row for merge
      case false =>
        return true
    }

  }


  private def enterQ() = {
    fileIterators.foreach(file => file._1)
  }

  override def get() = ???

  override def close() = ???
}
