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

package org.apache.spark.sql.execution.datasources.v2.merge.parquet.batch

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.{ArrayData, MapData}
import org.apache.spark.sql.execution.datasources.v2.merge.parquet.batch.merge_operator.{DefaultMergeOp, MergeOperator}
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.{ColumnVector, ColumnarBatch}
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

class MergeOperatorColumnarBatchRow(columns: Array[ColumnVector],
                                    mergeOps: Seq[MergeOperator[Any]],
                                    indexTypeArray: Seq[(Int, DataType)]) extends MergeBatchRow {

  val size: Int = indexTypeArray.length
  var idMix: Seq[Seq[(Int, Int)]] = _
  var value: Array[Any] = new Array[Any](size)

  private def getIndex(ordinal: Int): Seq[(Int, Int)] = {
    idMix(ordinal)
  }


  //merge data from idMix
  def mergeValues(): Unit ={
    idMix.zipWithIndex.foreach(m => {
      if(m._1.nonEmpty){
        val dataType = indexTypeArray(m._2)._2
        dataType match {
          case StringType => mergeUTF8String(m._1, m._2)
          case IntegerType | DateType => mergeInt(m._1, m._2)
          case BooleanType => mergeBoolean(m._1, m._2)
          case ByteType => mergeBoolean(m._1, m._2)
          case ShortType => mergeShort(m._1, m._2)
          case LongType | TimestampType => mergeLong(m._1, m._2)
          case FloatType => mergeFloat(m._1, m._2)
          case DoubleType => mergeDouble(m._1, m._2)
          case BinaryType => mergeBinary(m._1, m._2)
          case CalendarIntervalType => mergeInterval(m._1, m._2)
          case t: DecimalType => mergeDecimal(m._1, m._2, t.precision, t.scale)
          case t: StructType => mergeStruct(m._1, m._2, t.size)
          case _: ArrayType => mergeArray(m._1, m._2)
          case _: MapType => mergeMap(m._1, m._2)
          case o => throw new UnsupportedOperationException(s"StarLake MergeOperator don't support type ${o.typeName}")
        }
      }
    })
  }

  private def getMergeOp(ordinal: Int): MergeOperator[Any] = {
    mergeOps(ordinal)
  }

  override def numFields(): Int = {
    idMix.length
  }

  override def copy(): InternalRow = {
    val row: GenericInternalRow = new GenericInternalRow(idMix.length)
    (0 to numFields()).foreach(i => {
      if (isNullAt(i)) {
        row.setNullAt(i)
      } else {
        val colIdAndRowId: Seq[(Int, Int)] = getIndex(i)
        val dt = columns(colIdAndRowId.head._1).dataType()
        setRowData(i, dt, row)
      }
    })
    row
  }


  override def anyNull: Boolean = {
    throw new UnsupportedOperationException()
  }

  override def isNullAt(ordinal: Int): Boolean = {
    getIndex(ordinal).isEmpty || value(ordinal) == null
  }


  override def getBoolean(ordinal: Int): Boolean = {
    value(ordinal).asInstanceOf[Boolean]
  }

  override def getByte(ordinal: Int): Byte = {
    value(ordinal).asInstanceOf[Byte]
  }

  override def getShort(ordinal: Int): Short = {
    value(ordinal).asInstanceOf[Short]
  }

  override def getInt(ordinal: Int): Int = {
    value(ordinal).asInstanceOf[Int]
  }

  override def getLong(ordinal: Int): Long = {
    value(ordinal).asInstanceOf[Long]
  }

  override def getFloat(ordinal: Int): Float = {
    value(ordinal).asInstanceOf[Float]
  }

  override def getDouble(ordinal: Int): Double = {
    value(ordinal).asInstanceOf[Double]
  }

  override def getDecimal(ordinal: Int, precision: Int, scale: Int): Decimal = {
    value(ordinal).asInstanceOf[Decimal]
  }

  override def getUTF8String(ordinal: Int): UTF8String = {
    value(ordinal).asInstanceOf[UTF8String]
  }

  override def getBinary(ordinal: Int): Array[Byte] = {
    value(ordinal).asInstanceOf[Array[Byte]]
  }

  override def getInterval(ordinal: Int): CalendarInterval = {
    value(ordinal).asInstanceOf[CalendarInterval]
  }

  override def getStruct(ordinal: Int, numFields: Int): InternalRow = {
    value(ordinal).asInstanceOf[InternalRow]
  }

  override def getArray(ordinal: Int): ArrayData = {
    value(ordinal).asInstanceOf[ArrayData]
  }

  override def getMap(ordinal: Int): MapData = {
    value(ordinal).asInstanceOf[MapData]
  }

  override def update(i: Int, value: Any): Unit = {
    throw new UnsupportedOperationException()
  }

  override def setNullAt(i: Int): Unit = {
    throw new UnsupportedOperationException()
  }





  /** merge values */

  def mergeBoolean(colIdAndRowId: Seq[(Int,Int)], ordinal: Int): Unit = {
    if(getMergeOp(ordinal).isInstanceOf[DefaultMergeOp[Any]]){
      if (columns(colIdAndRowId.last._1).isNullAt(colIdAndRowId.last._2)){
        value(ordinal) = null
      }else{
        value(ordinal) = columns(colIdAndRowId.last._1).getBoolean(colIdAndRowId.last._2)
      }
    }else{
      val data = colIdAndRowId.map(m => {
        if(columns(m._1).isNullAt(m._2)) {
          null
        } else {
          columns(m._1).getBoolean(m._2)
        }
      }).asInstanceOf[Seq[Boolean]]

      val mergeOp = getMergeOp(ordinal).asInstanceOf[MergeOperator[Boolean]]
      value(ordinal) = mergeOp.mergeData(data)
    }
  }

  def mergeByte(colIdAndRowId: Seq[(Int,Int)], ordinal: Int): Unit = {
    if(getMergeOp(ordinal).isInstanceOf[DefaultMergeOp[Any]]){
      if (columns(colIdAndRowId.last._1).isNullAt(colIdAndRowId.last._2)){
        value(ordinal) = null
      }else{
        value(ordinal) = columns(colIdAndRowId.last._1).getByte(colIdAndRowId.last._2)
      }
    }else{
      val data = colIdAndRowId.map(m => {
        if(columns(m._1).isNullAt(m._2)) {
          null
        } else {
          columns(m._1).getByte(m._2)
        }
      }).asInstanceOf[Seq[Byte]]

      val mergeOp = getMergeOp(ordinal).asInstanceOf[MergeOperator[Byte]]
      value(ordinal) = mergeOp.mergeData(data)
    }
  }

  def mergeShort(colIdAndRowId: Seq[(Int,Int)], ordinal: Int): Unit = {
    if(getMergeOp(ordinal).isInstanceOf[DefaultMergeOp[Any]]){
      if (columns(colIdAndRowId.last._1).isNullAt(colIdAndRowId.last._2)){
        value(ordinal) = null
      }else{
        value(ordinal) = columns(colIdAndRowId.last._1).getShort(colIdAndRowId.last._2)
      }
    }else{
      val data = colIdAndRowId.map(m => {
        if(columns(m._1).isNullAt(m._2)) {
          null
        } else {
          columns(m._1).getShort(m._2)
        }
      }).asInstanceOf[Seq[Short]]

      val mergeOp = getMergeOp(ordinal).asInstanceOf[MergeOperator[Short]]
      value(ordinal) = mergeOp.mergeData(data)
    }
  }

  def mergeInt(colIdAndRowId: Seq[(Int,Int)], ordinal: Int): Unit = {
    if(getMergeOp(ordinal).isInstanceOf[DefaultMergeOp[Any]]){
      if (columns(colIdAndRowId.last._1).isNullAt(colIdAndRowId.last._2)){
        value(ordinal) = null
      }else{
        value(ordinal) = columns(colIdAndRowId.last._1).getInt(colIdAndRowId.last._2)
      }
    }else{
      val data = colIdAndRowId.map(m => {
        if(columns(m._1).isNullAt(m._2)) {
          null
        } else {
          columns(m._1).getInt(m._2)
        }
      }).asInstanceOf[Seq[Int]]

      val mergeOp = getMergeOp(ordinal).asInstanceOf[MergeOperator[Int]]
      value(ordinal) = mergeOp.mergeData(data)
    }
  }

  def mergeLong(colIdAndRowId: Seq[(Int,Int)], ordinal: Int): Unit = {
    if(getMergeOp(ordinal).isInstanceOf[DefaultMergeOp[Any]]){
      if (columns(colIdAndRowId.last._1).isNullAt(colIdAndRowId.last._2)) {
        value(ordinal) = null
      }else{
        value(ordinal) = columns(colIdAndRowId.last._1).getLong(colIdAndRowId.last._2)
      }
    }else{
      val data = colIdAndRowId.map(m => {
        if(columns(m._1).isNullAt(m._2)) {
          null
        } else {
          columns(m._1).getLong(m._2)
        }
      }).asInstanceOf[Seq[Int]]

      val mergeOp = getMergeOp(ordinal).asInstanceOf[MergeOperator[Int]]
      value(ordinal) = mergeOp.mergeData(data)
    }
  }

  def mergeFloat(colIdAndRowId: Seq[(Int,Int)], ordinal: Int): Unit = {
    if(getMergeOp(ordinal).isInstanceOf[DefaultMergeOp[Any]]){
      if (columns(colIdAndRowId.last._1).isNullAt(colIdAndRowId.last._2)){
        value(ordinal) = null
      }else{
        value(ordinal) = columns(colIdAndRowId.last._1).getFloat(colIdAndRowId.last._2)
      }
    }else{
      val data = colIdAndRowId.map(m => {
        if(columns(m._1).isNullAt(m._2)) {
          null
        } else {
          columns(m._1).getFloat(m._2)
        }
      }).asInstanceOf[Seq[Float]]

      val mergeOp = getMergeOp(ordinal).asInstanceOf[MergeOperator[Float]]
      value(ordinal) = mergeOp.mergeData(data)
    }
  }

  def mergeDouble(colIdAndRowId: Seq[(Int,Int)], ordinal: Int): Unit = {
    if(getMergeOp(ordinal).isInstanceOf[DefaultMergeOp[Any]]){
      if (columns(colIdAndRowId.last._1).isNullAt(colIdAndRowId.last._2)){
        value(ordinal) = null
      }else{
        value(ordinal) = columns(colIdAndRowId.last._1).getDouble(colIdAndRowId.last._2)
      }
    }else{
      val data = colIdAndRowId.map(m => {
        if(columns(m._1).isNullAt(m._2)) {
          null
        } else {
          columns(m._1).getDouble(m._2)
        }
      }).asInstanceOf[Seq[Double]]

      val mergeOp = getMergeOp(ordinal).asInstanceOf[MergeOperator[Double]]
      value(ordinal) = mergeOp.mergeData(data)
    }
  }

  def mergeDecimal(colIdAndRowId: Seq[(Int,Int)], ordinal: Int, precision: Int, scale: Int): Unit = {
    if(getMergeOp(ordinal).isInstanceOf[DefaultMergeOp[Any]]){
      if (columns(colIdAndRowId.last._1).isNullAt(colIdAndRowId.last._2)){
        value(ordinal) = null
      }else{
        value(ordinal) = columns(colIdAndRowId.last._1).getDecimal(colIdAndRowId.last._2, precision, scale)
      }
    }else{
      val data = colIdAndRowId.map(m => {
        if(columns(m._1).isNullAt(m._2)) {
          null
        } else {
          columns(m._1).getDecimal(colIdAndRowId.last._2, precision, scale)
        }
      })

      val mergeOp = getMergeOp(ordinal).asInstanceOf[MergeOperator[Decimal]]
      value(ordinal) = mergeOp.mergeData(data)
    }
  }

  def mergeUTF8String(colIdAndRowId: Seq[(Int,Int)], ordinal: Int): Unit = {
    if(getMergeOp(ordinal).isInstanceOf[DefaultMergeOp[Any]]){
      if (columns(colIdAndRowId.last._1).isNullAt(colIdAndRowId.last._2)){
        value(ordinal) = null
      }else{
        value(ordinal) = columns(colIdAndRowId.last._1).getUTF8String(colIdAndRowId.last._2)
      }
    }else{
      val data = colIdAndRowId.map(m => {
        if(columns(m._1).isNullAt(m._2)) {
          null
        } else {
          columns(m._1).getUTF8String(m._2).toString
        }
      })

      val mergeOp = getMergeOp(ordinal).asInstanceOf[MergeOperator[String]]
      value(ordinal) = UTF8String.fromString(mergeOp.mergeData(data))
    }
  }

  def mergeBinary(colIdAndRowId: Seq[(Int,Int)], ordinal: Int): Unit = {
    if(getMergeOp(ordinal).isInstanceOf[DefaultMergeOp[Any]]){
      if (columns(colIdAndRowId.last._1).isNullAt(colIdAndRowId.last._2)){
        value(ordinal) = null
      }else{
        value(ordinal) = columns(colIdAndRowId.last._1).getBinary(colIdAndRowId.last._2)
      }
    }else{
      val data = colIdAndRowId.map(m => {
        if(columns(m._1).isNullAt(m._2)) {
          null
        } else {
          columns(m._1).getBinary(m._2)
        }
      })

      val mergeOp = getMergeOp(ordinal).asInstanceOf[MergeOperator[Array[Byte]]]
      value(ordinal) = mergeOp.mergeData(data)
    }
  }

  def mergeInterval(colIdAndRowId: Seq[(Int,Int)], ordinal: Int): Unit = {
    if(getMergeOp(ordinal).isInstanceOf[DefaultMergeOp[Any]]){
      if (columns(colIdAndRowId.last._1).isNullAt(colIdAndRowId.last._2)){
        value(ordinal) = null
      }else{
        value(ordinal) = columns(colIdAndRowId.last._1).getInterval(colIdAndRowId.last._2)
      }
    }else{
      val data = colIdAndRowId.map(m => {
        if(columns(m._1).isNullAt(m._2)) {
          null
        } else {
          columns(m._1).getInterval(m._2)
        }
      })

      val mergeOp = getMergeOp(ordinal).asInstanceOf[MergeOperator[CalendarInterval]]
      value(ordinal) = mergeOp.mergeData(data)
    }
  }

  def mergeStruct(colIdAndRowId: Seq[(Int,Int)], ordinal: Int, numFields: Int): Unit = {
    if(getMergeOp(ordinal).isInstanceOf[DefaultMergeOp[Any]]){
      if (columns(colIdAndRowId.last._1).isNullAt(colIdAndRowId.last._2)){
        value(ordinal) = null
      }else{
        value(ordinal) = columns(colIdAndRowId.last._1).getStruct(colIdAndRowId.last._2)
      }
    }else{
      val data = colIdAndRowId.map(m => {
        if(columns(m._1).isNullAt(m._2)) {
          null
        } else {
          columns(m._1).getStruct(m._2)
        }
      })

      val mergeOp = getMergeOp(ordinal).asInstanceOf[MergeOperator[InternalRow]]
      value(ordinal) = mergeOp.mergeData(data)
    }
  }

  def mergeArray(colIdAndRowId: Seq[(Int,Int)], ordinal: Int): Unit = {
    if(getMergeOp(ordinal).isInstanceOf[DefaultMergeOp[Any]]){
      if (columns(colIdAndRowId.last._1).isNullAt(colIdAndRowId.last._2)){
        value(ordinal) = null
      }else{
        value(ordinal) = columns(colIdAndRowId.last._1).getArray(colIdAndRowId.last._2)
      }
    }else{
      val data = colIdAndRowId.map(m => {
        if(columns(m._1).isNullAt(m._2)) {
          null
        } else {
          columns(m._1).getArray(m._2)
        }
      })

      val mergeOp = getMergeOp(ordinal).asInstanceOf[MergeOperator[ArrayData]]
      value(ordinal) = mergeOp.mergeData(data)
    }
  }

  def mergeMap(colIdAndRowId: Seq[(Int,Int)], ordinal: Int): Unit = {
    if(getMergeOp(ordinal).isInstanceOf[DefaultMergeOp[Any]]){
      if (columns(colIdAndRowId.last._1).isNullAt(colIdAndRowId.last._2)){
        value(ordinal) = null
      }else{
        value(ordinal) = columns(colIdAndRowId.last._1).getMap(colIdAndRowId.last._2)
      }
    }else{
      val data = colIdAndRowId.map(m => {
        if(columns(m._1).isNullAt(m._2)) {
          null
        } else {
          columns(m._1).getMap(m._2)
        }
      })

      val mergeOp = getMergeOp(ordinal).asInstanceOf[MergeOperator[MapData]]
      value(ordinal) = mergeOp.mergeData(data)
    }
  }




  ///////////////////////////////////////////////////////////////////////////////////


  /** get values need to be merged */

  def getMergeBoolean(ordinal: Int): Seq[Boolean] = {
    val colIdAndRowId = getIndex(ordinal)

    colIdAndRowId.map(m => {
      if(columns(m._1).isNullAt(m._2)) {
        null
      } else {
        columns(m._1).getBoolean(m._2)
      }
    }).asInstanceOf[Seq[Boolean]]
  }

  def getMergeByte(ordinal: Int): Seq[Byte] = {
    val colIdAndRowId = getIndex(ordinal)

    colIdAndRowId.map(m => {
      if(columns(m._1).isNullAt(m._2)) {
        null
      } else {
        columns(m._1).getByte(m._2)
      }
    }).asInstanceOf[Seq[Byte]]
  }

  def getMergeShort(ordinal: Int): Seq[Short] = {
    val colIdAndRowId = getIndex(ordinal)

    colIdAndRowId.map(m => {
      if(columns(m._1).isNullAt(m._2)) {
        null
      } else {
        columns(m._1).getShort(m._2)
      }
    }).asInstanceOf[Seq[Short]]
  }

  def getMergeInt(ordinal: Int): Seq[Int] = {
    val colIdAndRowId = getIndex(ordinal)

    colIdAndRowId.map(m => {
      if(columns(m._1).isNullAt(m._2)) {
        null
      } else {
        columns(m._1).getInt(m._2)
      }
    }).asInstanceOf[Seq[Int]]
  }

  def getMergeLong(ordinal: Int): Seq[Long] = {
    val colIdAndRowId = getIndex(ordinal)

    colIdAndRowId.map(m => {
      if(columns(m._1).isNullAt(m._2)) {
        null
      } else {
        columns(m._1).getLong(m._2)
      }
    }).asInstanceOf[Seq[Long]]
  }

  def getMergeFloat(ordinal: Int): Seq[Float] = {
    val colIdAndRowId = getIndex(ordinal)

    colIdAndRowId.map(m => {
      if(columns(m._1).isNullAt(m._2)) {
        null
      } else {
        columns(m._1).getFloat(m._2)
      }
    }).asInstanceOf[Seq[Float]]
  }

  def getMergeDouble(ordinal: Int): Seq[Double] = {
    val colIdAndRowId = getIndex(ordinal)

    colIdAndRowId.map(m => {
      if(columns(m._1).isNullAt(m._2)) {
        null
      } else {
        columns(m._1).getDouble(m._2)
      }
    }).asInstanceOf[Seq[Double]]
  }

  def getMergeDecimal(ordinal: Int, precision: Int, scale: Int): Seq[Decimal] = {
    val colIdAndRowId = getIndex(ordinal)

    colIdAndRowId.map(m => {
      if(columns(m._1).isNullAt(m._2)) {
        null
      } else {
        columns(m._1).getDecimal(m._2, precision, scale)
      }
    })
  }

  def getMergeUTF8String(ordinal: Int): Seq[UTF8String] = {
    val colIdAndRowId = getIndex(ordinal)

    colIdAndRowId.map(m => {
      if(columns(m._1).isNullAt(m._2)) {
        null
      } else {
        columns(m._1).getUTF8String(m._2).clone()
      }
    })
  }

  def getMergeBinary(ordinal: Int): Seq[Array[Byte]] = {
    val colIdAndRowId = getIndex(ordinal)
    columns(colIdAndRowId.head._1).getBinary(colIdAndRowId.head._2)

    colIdAndRowId.map(m => {
      if(columns(m._1).isNullAt(m._2)) {
        null
      } else {
        columns(m._1).getBinary(m._2)
      }
    })
  }

  def getMergeInterval(ordinal: Int): Seq[CalendarInterval] = {
    val colIdAndRowId = getIndex(ordinal)
    columns(colIdAndRowId.head._1).getInterval(colIdAndRowId.head._2)

    colIdAndRowId.map(m => {
      if(columns(m._1).isNullAt(m._2)) {
        null
      } else {
        columns(m._1).getInterval(m._2)
      }
    })
  }

  def getMergeStruct(ordinal: Int, numFields: Int): Seq[InternalRow] = {
    val colIdAndRowId = getIndex(ordinal)

    colIdAndRowId.map(m => {
      if(columns(m._1).isNullAt(m._2)) {
        null
      } else {
        columns(m._1).getStruct(m._2)
      }
    })
  }

  def getMergeArray(ordinal: Int): Seq[ArrayData] = {
    val colIdAndRowId = getIndex(ordinal)
    columns(colIdAndRowId.head._1).getArray(colIdAndRowId.head._2)

    colIdAndRowId.map(m => {
      if(columns(m._1).isNullAt(m._2)) {
        null
      } else {
        columns(m._1).getArray(m._2)
      }
    })
  }

  def getMergeMap(ordinal: Int): Seq[MapData] = {
    val colIdAndRowId = getIndex(ordinal)

    colIdAndRowId.map(m => {
      if(columns(m._1).isNullAt(m._2)) {
        null
      } else {
        columns(m._1).getMap(m._2)
      }
    })
  }


}
