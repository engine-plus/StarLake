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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.v2.merge.parquet.batch.MergeOperatorColumnarBatchRow
import org.apache.spark.sql.types.DataType
import org.apache.spark.sql.vectorized.ColumnVector

class MergeColumnarBatchNew(columns: Array[ColumnVector],
                            mergeOps: Seq[MergeOperator[Any]],
                            indexTypeArray: Seq[(Int, DataType)]) extends AutoCloseable {

  val row = new MergeOperatorColumnarBatchRow(columns, mergeOps, indexTypeArray)

  def getRow(resultIndex: Seq[Seq[(Int, Int)]]): InternalRow = {
    row.idMix = resultIndex
    row.mergeValues()
    row
  }

  def getMergeRow(resultIndex: Seq[Seq[(Int, Int)]]): MergeOperatorColumnarBatchRow = {
    row.idMix = resultIndex
    row.mergeValues()
    row
  }

  override def close(): Unit = {
    for (c <- columns) {
      c.close()
    }
  }


}
