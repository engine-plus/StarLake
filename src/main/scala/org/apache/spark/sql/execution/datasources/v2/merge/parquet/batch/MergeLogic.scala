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

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.InternalRow.getAccessor
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.execution.datasources.v2.merge.MergePartitionedFile
import org.apache.spark.sql.types._
import org.apache.spark.sql.vectorized.ColumnarBatch

import scala.collection.{BufferedIterator, mutable}

trait MergeLogic {

  def getValueByType(row: InternalRow, fieldIndex: Int, dataType: DataType): Any = {
    dataType match {
      case StringType => if (row.isNullAt(fieldIndex)) null else row.getUTF8String(fieldIndex).clone()
      case IntegerType | DateType => if (row.isNullAt(fieldIndex)) null else row.getInt(fieldIndex)
      case BooleanType => if (row.isNullAt(fieldIndex)) null else row.getBoolean(fieldIndex)
      case ByteType => if (row.isNullAt(fieldIndex)) null else row.getByte(fieldIndex)
      case ShortType => if (row.isNullAt(fieldIndex)) null else row.getShort(fieldIndex)
      case LongType | TimestampType => if (row.isNullAt(fieldIndex)) null else row.getLong(fieldIndex)
      case FloatType => if (row.isNullAt(fieldIndex)) null else row.getFloat(fieldIndex)
      case DoubleType => if (row.isNullAt(fieldIndex)) null else row.getDouble(fieldIndex)
      case BinaryType => if (row.isNullAt(fieldIndex)) null else row.getBinary(fieldIndex)
      case CalendarIntervalType => if (row.isNullAt(fieldIndex)) null else row.getInterval(fieldIndex)
      case t: DecimalType => if (row.isNullAt(fieldIndex)) null else row.getDecimal(fieldIndex, t.precision, t.scale)
      case t: StructType => if (row.isNullAt(fieldIndex)) null else row.getStruct(fieldIndex, t.size)
      case _: ArrayType => if (row.isNullAt(fieldIndex)) null else row.getArray(fieldIndex)
      case _: MapType => if (row.isNullAt(fieldIndex)) null else row.getMap(fieldIndex)
      case u: UserDefinedType[_] => getAccessor(u.sqlType, true)
      case _ => if (row.isNullAt(fieldIndex)) null else row.get(fieldIndex, dataType)
    }
  }

}

//class MergeMultipleFile(filesInfo: Seq[(MergePartitionedFile, PartitionReader[ColumnarBatch])]) extends MergeLogic {
//
//  var temporaryStoreLastRow = false
//
//  /**
//    * Get the key information of each merge file And build it into a Map.
//    * Map[version->keyInfo]
//    * keyInfo:Array[(Int,DataType)]
//    */
//  var versionKeyInfoMap: Map[Long, Array[(Int, DataType)]] = filesInfo.map(info => {
//    info._1.writeVersion -> info._1.keyInfo.toArray
//  }).toMap //.fold(Map[String, Seq(Int,DataType)]())(_ ++ _)
//
//  /**
//    * The corresponding index of each field in mergeRow.
//    * Map:[fieldName->index] , index: Indicates the position of the field after the merge
//    */
//  val fieldIndexMap: Map[String, Int] = filesInfo.head._1.resultSchema.zipWithIndex.map(field_index => {
//    field_index._1._1 -> field_index._2
//  }).toMap
//
//  /**
//    * Replace the field name with the index
//    * The order of the array elements represents the order of the entire mergeRow elements。
//    */
//  val indexTypeArray: Array[(Int, DataType)] = filesInfo.head._1.resultSchema.map(info => (fieldIndexMap(info._1), info._2)).toArray
//
//  /**
//    * Get the file information of each merge file And build it into a Map.
//    * Map[version->fileInfo]
//    * fileInfo:Array[(Int,DataType)]
//    */
//  val versionFileInfoMap: Map[Long, Array[(Int, DataType)]] = filesInfo.map(t => {
//    t._1.writeVersion -> t._1.fileInfo.map(info => (fieldIndexMap(info._1), info._2)).toArray
//  }).toMap
//
//  // Initialize the proxy mergeRow object
//  private val resultIndex = new Array[(Integer, Integer)](filesInfo.head._1.resultSchema.length)
//
//  //Initialize the last piece of data when the batch is switched
//  var temporaryRow: Array[Any] = new Array[Any](filesInfo.head._1.resultSchema.length)
//
//  //get next batch
//  val fileSeq: Seq[(MergePartitionedFile, ColumnarBatch)] = MergeUtils.getNextBatch(filesInfo)
//  val mergeHeap = new MergeOptimizeHeap(versionKeyInfoMap)
//  mergeHeap.enqueueBySeq(MergeUtils.toBufferdIterator(fileSeq))
//
//  /** initialize mergeBatchColumnIndex and mergeColumnarBatch object */
//  val mergeColumnIndexMap: mutable.Map[Long, Array[Int]] = mutable.Map[Long, Array[Int]]()
//  //initialize mergeBatchColumnIndex, create column index for all files by write version
//  MergeUtils.initMergeBatchAndMergeIndex(fileSeq, mergeColumnIndexMap)
//  //initialize mergeColumnarBatch object
//  val mergeColumnarBatch: MergeColumnarBatch = MergeUtils.initMergeBatch(fileSeq)
//
//
//  def getRowByProxyMergeBatch(): InternalRow = {
//    mergeColumnarBatch.getRow(resultIndex)
//  }
//
//  def isTemporaryRow(): Boolean = temporaryStoreLastRow
//
//  def isStorgeBatch(): Boolean = {
//    resultIndex.head._1 != -1
//  }
//
//  def getTemporaryRow(): Array[Any] = temporaryRow
//
//  def setTemporaryRowFalse(): Unit = {
//    temporaryStoreLastRow = false
//  }
//
//  def isHeapEmpty: Boolean = mergeHeap.isEmpty
//
//  def merge(): Unit = {
//
//    MergeUtils.resetBatchIndex(resultIndex)
//    var lastKey: String = null
//    while (mergeHeap.nonEmpty) {
//      val currentFile = mergeHeap.dequeue()
//      val currentRowAndLineId = currentFile._2.head
//      val currentVersion = currentFile._1
//
//      if (StringUtils.isEmpty(lastKey)) {
//        lastKey = combineKey(currentVersion, currentRowAndLineId._1)
//      } else {
//        if (!combineKey(currentVersion, currentRowAndLineId._1).equals(lastKey)) {
//          mergeHeap.enqueue(currentFile)
//          lastKey = null
//          // End the merge
//          return
//        }
//      }
//
//      if (temporaryStoreLastRow) {
//        storeRow(currentVersion, currentRowAndLineId._1)
//        if (isStorgeBatch()) MergeUtils.resetBatchIndex(resultIndex)
//      }
//
//      currentFile._2.next() //指针向下移动（迭代器指针和 head 指针）
//      if(currentFile._2.hasNext){
//        mergeHeap.enqueue(currentFile)
//        //calculate the field index in File And fill into the MergeBatch Object
//        if (!temporaryStoreLastRow) fillMergeBatchIndex(currentRowAndLineId, currentVersion)
//      }else{
//        //storge the last row of batch
//        if (!temporaryStoreLastRow) {
//          storageBatchLastRow(currentVersion, currentRowAndLineId._1)
//          temporaryStoreLastRow = true
//        }
//
//        val fileInfo = filesInfo.filter(t => t._1.writeVersion.equals(currentVersion))
//        val nextBatchs = MergeUtils.getNextBatch(fileInfo)
//
//        if (nextBatchs.nonEmpty) {
//          val bufferIt = MergeUtils.toBufferdIterator(nextBatchs)
//          mergeHeap.enqueue(bufferIt.head)
//          updateMergeBatchColumn(nextBatchs.head)
//        } else {
//          mergeHeap.poll()
//        }
//      }
//
//
//    }
//
//  }
//
//  def storageBatchLastRow(currentVersion: Long, row: InternalRow): Unit =  {
//    //first, If the mergeBatch has been updated,first save the row in the mergeBatch
//    if (isStorgeBatch()) {
//      storeRowByMergeBatch(mergeColumnarBatch.getRow(resultIndex))
//      temporaryStoreLastRow = true
//      MergeUtils.resetBatchIndex(resultIndex)
//    }
//    //then, save the current row
//    storeRow(currentVersion, row)
//  }
//
//  def storeRowByMergeBatch(internalRow: InternalRow): Unit = {
//    for (i <- resultIndex.indices) {
//      temporaryRow(i) =
//        if (resultIndex(i)._1 == -1) {
//          null
//        } else {
//          val fieldType = indexTypeArray(i)
//          getValueByType(internalRow, i, fieldType._2)
//        }
//    }
//  }
//
//
//  def storeRow(version: Long, row: InternalRow): Unit = {
//    for (i <- versionFileInfoMap(version).indices) {
//      val fieldType = versionFileInfoMap(version)(i)
//      temporaryRow(fieldType._1) = getValueByType(row, i, fieldType._2)
//    }
//  }
//
//
//  def fillMergeBatchIndex(rowAndId: (InternalRow, Int), writerVersion: Long): Unit = {
//    //get fileInfo ,Array[0,3,4] -> a,d,e
//    val columns = versionFileInfoMap(writerVersion)
//    //get field Index for MergeBatch Object
//    val mergeBatchIndex = mergeColumnIndexMap(writerVersion)
//
//    columns.indices.foreach(i => resultIndex(columns(i)._1) = (mergeBatchIndex(i), rowAndId._2))
//  }
//
//  def combineKey(version: Long, row: InternalRow): String = {
//    versionKeyInfoMap(version)
//      .map(key_type => {
//        row.get(key_type._1, key_type._2).toString
//      })
//      .reduce(_.concat(_))
//  }
//
//
//  def updateMergeBatchColumn(newBatch: (MergePartitionedFile, ColumnarBatch)): Unit = {
//    val updateIndex = mergeColumnIndexMap(newBatch._1.writeVersion)
//    mergeColumnarBatch.updateBatch(newBatch._2, updateIndex)
//  }
//
//
//}

import scala.collection.JavaConverters._

class MergeSingletonFile(filesInfo: Seq[(MergePartitionedFile, PartitionReader[ColumnarBatch])]) extends MergeLogic {

  //initialize index
  val keyInfoArray: Array[(Int, DataType)] = filesInfo.head._1.keyInfo.toArray

  val typeArray: Array[DataType] = filesInfo.head._1.fileInfo.map(_._2).toArray

  var temporaryRow: Array[Any] = new Array[Any](filesInfo.head._1.resultSchema.length)
  //  //get next batch
  var fileSeq: Seq[(MergePartitionedFile, ColumnarBatch)] = MergeUtils.getNextBatch(filesInfo)

  val fileSchema: Seq[String] = filesInfo.head._1.fileInfo.map(_._1)
  val resIndex: Array[Int] = filesInfo.head._1.resultSchema.map(_._1).map(schema => {
    fileSchema.indexOf(schema)
  }).toArray

  var singletonBatch: SingletonFileColumnarBatch = MergeUtils.initMergeBatch(fileSeq.head, resIndex)

  var temporaryStoreLastRow = false

  var bufferedIt: BufferedIterator[(InternalRow, Int)] = fileSeq.head._2.rowIterator().asScala.zipWithIndex.buffered
  var rowId: Int = -1

  def deDuplication(): Boolean = {
    var lastKey: String = null
    rowId = -1
    while (true) {
      if (bufferedIt.hasNext) {
        val currentRow = bufferedIt.head._1
        if(StringUtils.isEmpty(lastKey)){
          lastKey = combineKey(currentRow)
          rowId = bufferedIt.head._2
        }else{
          if (combineKey(currentRow).equals(lastKey)) {
            if (temporaryStoreLastRow) temporaryStoreLastRow = false
            rowId = bufferedIt.head._2
          } else {
            return true
          }
        }
        bufferedIt.next()
      } else {
        if (rowId == -1) return false
        val tempRow = getRow()
        resIndex.indices.foreach(i => {
          if (resIndex(i) == -1) {
            temporaryRow(i) = null
          } else {
            temporaryRow(i) = getValueByType(tempRow, i, typeArray(resIndex(i)))
          }
        })
        temporaryStoreLastRow = true
        fileSeq = MergeUtils.getNextBatch(filesInfo)
        if (fileSeq.nonEmpty) {
          bufferedIt = fileSeq.head._2.rowIterator().asScala.zipWithIndex.buffered
          singletonBatch = MergeUtils.initMergeBatch(fileSeq.head, resIndex)
        } else {
          return true
        }
      }

    }
    false
  }

  def combineKey(row: InternalRow): String = {
    keyInfoArray.map(keyType => {
      row.get(keyType._1, keyType._2).toString
    })
      .reduce(_.concat(_))
  }

  def getRow(): InternalRow = {
    if (temporaryStoreLastRow) {
      val batchLastRow = new GenericInternalRow(temporaryRow.clone())
      temporaryRow.indices.foreach(temporaryRow(_) = null)
      temporaryStoreLastRow = false
      batchLastRow
    } else {
      singletonBatch.getRow(rowId)
    }
  }

}


