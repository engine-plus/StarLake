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
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.execution.datasources.v2.merge.MergePartitionedFile
import org.apache.spark.sql.vectorized.ColumnarBatch

import scala.collection.JavaConverters._
import scala.collection.{BufferedIterator, mutable}

/**
  * Created by luozhenglin 
  * on 2021/3/2 11:39 AM
  */
object MergeUtils {

  /**
    * to Buffer Itrator
    *
    * @param seq
    * @return
    */
  def toBufferdIterator(seq: Seq[(MergePartitionedFile, ColumnarBatch)]): Seq[(Long, BufferedIterator[(InternalRow, Int)])] = {
    seq.map(tuple => tuple._1.writeVersion -> tuple._2.rowIterator().asScala.zipWithIndex.buffered)
  }

  /**
    * @param filesInfo
    * @return
    */
  def getNextBatch(filesInfo: Seq[(MergePartitionedFile, PartitionReader[ColumnarBatch])]):
  Seq[(MergePartitionedFile, ColumnarBatch)] = {
    filesInfo.filter(fileInfo_asynReader => fileInfo_asynReader._2.next())
      .map(fileInfo_asynReader => fileInfo_asynReader._1 -> fileInfo_asynReader._2.get())
  }

  /**
    * initialize mergeColumnarsBatch object and mergeBatchColumnIndex
    *
    */
  def initMergeBatchAndMergeIndex(fileSeq: Seq[(MergePartitionedFile, ColumnarBatch)],
                                  mergeColumnIndexMap: mutable.Map[Long, Array[Int]]): Unit = {
    //initialize mergeBatchColumnIndex
    val versionNumsT = fileSeq.sortWith((t1, t2) => t1._1.writeVersion < t2._1.writeVersion)
      .toArray.map(t => (t._1.writeVersion, t._2.numCols()))

    var lastLen = 0
    for (i <- versionNumsT.indices) {
      var end: Int = 0
      for (j <- 0 to i) {
        end += versionNumsT(j)._2
      }
      if (i != 0) {
        lastLen += versionNumsT(i - 1)._2
      }
      mergeColumnIndexMap += versionNumsT(i)._1 -> Range(lastLen, end).toArray
    }

  }

  def initMergeBatch(fileSeq: Seq[(MergePartitionedFile, ColumnarBatch)]): MergeColumnarsBatch = {
    //initialize mergeColumnarsBatch object
    val arrayColumn =
      fileSeq.sortWith((t1, t2) => t1._1.writeVersion < t2._1.writeVersion).toArray
        .map(t => {
          Range(0, t._2.numCols()).map(t._2.column)
        })
        .flatMap(_.toSeq)
    new MergeColumnarsBatch(arrayColumn)
  }

  def initMergeBatch(file: (MergePartitionedFile, ColumnarBatch), resIndex: Array[Int]): SingletonFIleColumnarsBatch = {
    val columnArr =
      resIndex.map(res => {
        if (res == -1) {
          null
        } else {
          file._2.column(res)
        }
      })
    new SingletonFIleColumnarsBatch(columnArr)
  }

  def resetBatchIndex(resultIndex: Array[(Integer, Integer)]): Unit = {
    for (i <- resultIndex.indices) {
      resultIndex(i) = (-1, -1)
    }
  }


}
