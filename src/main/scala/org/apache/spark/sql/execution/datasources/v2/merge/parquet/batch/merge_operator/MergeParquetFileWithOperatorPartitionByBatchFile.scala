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

package org.apache.spark.sql.execution.datasources.v2.merge.parquet.batch.merge_operator

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.connector.read.PartitionReader
import org.apache.spark.sql.execution.datasources.v2.merge.MergePartitionedFile
import org.apache.spark.sql.vectorized.ColumnarBatch


class MergeParquetFileWithOperatorPartitionByBatchFile[T](filesInfo: Seq[Seq[(MergePartitionedFile, PartitionReader[ColumnarBatch])]],
                                                          mergeOperatorInfo: Map[String, MergeOperator[Any]])
  extends PartitionReader[InternalRow] with Logging {

  val filesItr: Iterator[Seq[(MergePartitionedFile, PartitionReader[ColumnarBatch])]] = filesInfo.iterator
  var mergeLogic: MergeMultiFileWithOperator = _

  /**
    * @return Boolean
    */
  override def next(): Boolean = {

    if (mergeLogic == null) {
      if (filesItr.hasNext) {
        val nextFiles = filesItr.next()
        if (nextFiles.isEmpty) {
          return false
        } else {
          mergeLogic = new MergeMultiFileWithOperator(nextFiles, mergeOperatorInfo)
        }
      } else {
        return false
      }
    }

    if (mergeLogic.isHeapEmpty) {
      if (filesItr.hasNext) {
        mergeLogic = new MergeMultiFileWithOperator(filesItr.next(), mergeOperatorInfo)
      } else {
        return false
      }
    }

    mergeLogic.merge()
    true
  }

  /**
    * @return InternalRow
    */
  override def get(): InternalRow = {

    if (mergeLogic.isTemporaryRow()) {
      mergeLogic.setTemporaryRowFalse()
      val temporaryRow = mergeLogic.getTemporaryRow()
      val arrayRow = new GenericInternalRow(temporaryRow.clone())
      arrayRow
    } else {
      mergeLogic.getRowByProxyMergeBatch()
    }
  }

  override def close(): Unit = {
    if (filesInfo.nonEmpty) {
      filesInfo.foreach(f => f.foreach(_._2.close()))
    }
  }


}
