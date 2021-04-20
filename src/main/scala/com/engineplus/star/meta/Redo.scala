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

package com.engineplus.star.meta

import java.util.concurrent.TimeUnit

import com.engineplus.star.meta.UndoLog.{deleteUndoLogByCommitId, getUndoLogInfo, updateRedoTimestamp}
import org.apache.spark.internal.Logging

object Redo extends Logging{
  def redoCommit(table_name: String, table_id: String, commit_id: String): Boolean = {
    logInfo("redo other commit~~~   ")

    if (updateRedoTimestamp(table_id, commit_id)) {
      redoSchemaLock(table_name, table_id, commit_id)
      redoAddedFile(table_id, commit_id)
      redoExpiredFile(table_id, commit_id)
      redoPartitionLock(table_id, commit_id)
      redoStreamingRecord(table_id, commit_id)

      deleteUndoLogByCommitId(UndoLogType.Commit.toString, table_id, commit_id)
      true
    } else {
      TimeUnit.SECONDS.sleep(10)
      false
    }
  }

  private def redoPartitionLock(table_id: String, commit_id: String): Unit = {
    val partition_undo_arr = getUndoLogInfo(UndoLogType.Partition.toString, table_id, commit_id)
    for (partition_undo <- partition_undo_arr) {
      MetaVersion.updatePartitionInfo(partition_undo)
      MetaLock.unlock(partition_undo.range_id, partition_undo.commit_id)
    }
    deleteUndoLogByCommitId(UndoLogType.Partition.toString, table_id, commit_id)
  }

  private def redoSchemaLock(table_name: String, table_id: String, commit_id: String): Unit = {
    val schema_undo_arr = getUndoLogInfo(UndoLogType.Schema.toString, table_id, commit_id)

    for (schema_undo <- schema_undo_arr) {
      MetaVersion.updateTableSchema(
        table_name,
        table_id,
        commit_id,
        schema_undo.table_schema,
        schema_undo.setting,
        schema_undo.write_version.toInt)

      MetaLock.unlock(table_id, commit_id)
    }
    deleteUndoLogByCommitId(UndoLogType.Schema.toString, table_id, commit_id)
  }

  private def redoAddedFile(table_id: String, commit_id: String): Unit = {
    val add_file_undo_arr = getUndoLogInfo(UndoLogType.AddFile.toString, table_id, commit_id)
    for (add_file_undo <- add_file_undo_arr) {
      DataOperation.redoAddNewDataFile(add_file_undo)
    }
    deleteUndoLogByCommitId(UndoLogType.AddFile.toString, table_id, commit_id)
  }

  private def redoExpiredFile(table_id: String, commit_id: String): Unit = {
    val expire_file_undo_arr = getUndoLogInfo(UndoLogType.ExpireFile.toString, table_id, commit_id)
    for (expire_file_undo <- expire_file_undo_arr) {
      DataOperation.redoExpireDataFile(expire_file_undo)
    }
    deleteUndoLogByCommitId(UndoLogType.ExpireFile.toString, table_id, commit_id)
  }

  private def redoStreamingRecord(table_id: String, commit_id: String): Unit = {
    val streaming_undo_arr = getUndoLogInfo(UndoLogType.Commit.toString, table_id, commit_id)

    for (streaming_undo <- streaming_undo_arr) {
      if (streaming_undo.query_id.nonEmpty
        && !streaming_undo.query_id.equals(MetaUtils.UNDO_LOG_DEFAULT_VALUE)
        && streaming_undo.batch_id >= 0) {

        StreamingRecord.updateStreamingInfo(
          table_id,
          streaming_undo.query_id,
          streaming_undo.batch_id,
          streaming_undo.timestamp)
      }
    }
  }

}
