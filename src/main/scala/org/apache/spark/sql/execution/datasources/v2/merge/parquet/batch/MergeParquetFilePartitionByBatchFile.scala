///*
// * Copyright [2021] [EnginePlus Team]
// *
// * Licensed under the Apache License, Version 2.0 (the "License");
// * you may not use this file except in compliance with the License.
// * You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package org.apache.spark.sql.execution.datasources.v2.merge.parquet.batch
//
//import org.apache.spark.internal.Logging
//import org.apache.spark.sql.catalyst.InternalRow
//import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
//import org.apache.spark.sql.connector.read.PartitionReader
//import org.apache.spark.sql.execution.datasources.v2.merge.MergePartitionedFile
//import org.apache.spark.sql.vectorized.ColumnarBatch
//
//
//class MergeParquetFilePartitionByBatchFile[T](filesInfo: Seq[(MergePartitionedFile, PartitionReader[ColumnarBatch])])
//  extends PartitionReader[InternalRow] with Logging {
//
//  var mergeLogic: MergeMultipleFile = null
//
//  def createMergeLogic(): MergeMultipleFile = {
//    new MergeMultipleFile(filesInfo)
//  }
//
//  /**
//    * @return Boolean
//    */
//  override def next(): Boolean = {
//
//    if (filesInfo.isEmpty) return false
//    if (mergeLogic == null) mergeLogic = createMergeLogic()
//
//    if (mergeLogic.isHeapEmpty)
//      return false
//
//    mergeLogic.merge()
//    true
//  }
//
//  /**
//    * @return InternalRow
//    */
//  override def get(): InternalRow = {
//
//    if (mergeLogic.isTemporaryRow()) {
//      mergeLogic.setTemporaryRowFalse()
//      val temporaryRow = mergeLogic.getTemporaryRow()
//      val arrayRow = new GenericInternalRow(temporaryRow.clone())
//      temporaryRow.indices.foreach(temporaryRow(_) = null)
//      arrayRow
//    } else {
//      mergeLogic.getRowByProxyMergeBatch()
//    }
//
//  }
//
//  override def close() = {
//    if (filesInfo.nonEmpty) {
//      filesInfo.foreach(_._2.close())
//    }
//  }
//
//
//}
