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

import com.engineplus.star.meta.UndoLog._
import org.apache.spark.internal.Logging
import org.apache.spark.sql.star.utils.MetaInfo

object RollBack extends Logging{
  def rollBackUpdate(meta_info: MetaInfo, commit_id: String, changeSchema: Boolean): Unit = {
    val table_id = meta_info.table_info.table_id
    val (last_timestamp, tag) = getCommitTimestampAndTag(
      UndoLogType.Commit.toString,
      table_id,
      commit_id)

    if (tag != -1) {
      val partition_info_arr = meta_info.partitionInfoArray
      logInfo("============commit failed, commit id=" + commit_id)
      if (changeSchema) {
        MetaLock.unlock(table_id, commit_id)
        deleteUndoLogByCommitId(UndoLogType.Schema.toString, table_id, commit_id)
      }

      for (partition_info <- partition_info_arr) {
        MetaLock.unlock(partition_info.range_id, commit_id)
      }

      deleteUndoLogByCommitId(UndoLogType.AddFile.toString, table_id, commit_id)
      deleteUndoLogByCommitId(UndoLogType.ExpireFile.toString, table_id, commit_id)
      deleteUndoLogByCommitId(UndoLogType.Partition.toString, table_id, commit_id)
      deleteUndoLogByCommitId(UndoLogType.Commit.toString, table_id, commit_id)
    }
  }

  def rollBackCommit(table_id: String, commit_id: String, tag: Int, timestamp: Long): Unit = {
    logInfo("roll back other commit~~~   ")
    if (markOtherCommitRollBack(table_id, commit_id, tag, timestamp)) {
      rollBackSchemaLock(table_id, commit_id)
      rollBackAddedFile(table_id, commit_id)
      rollBackExpiredFile(table_id, commit_id)
      rollBackPartitionLock(table_id, commit_id)

      deleteUndoLogByCommitId(UndoLogType.Commit.toString, table_id, commit_id)
    } else {
      TimeUnit.SECONDS.sleep(10)
    }
  }

  def cleanRollBackCommit(table_id: String, commit_id: String, lock_id: String): Unit = {
    logInfo("clean roll back other commit~~~  ")

    rollBackSchemaLock(table_id, commit_id)
    rollBackAddedFile(table_id, commit_id)
    rollBackExpiredFile(table_id, commit_id)
    rollBackPartitionLock(table_id, commit_id)

    MetaLock.unlock(lock_id, commit_id)
  }


  def rollBackPartitionLock(table_id: String, commit_id: String): Unit = {
    val partition_undo_arr = getUndoLogInfo(UndoLogType.Partition.toString, table_id, commit_id)
    for (partition_undo <- partition_undo_arr) {
      MetaLock.unlock(partition_undo.range_id, commit_id)
    }
    deleteUndoLogByCommitId(UndoLogType.Partition.toString, table_id, commit_id)
  }

  private def rollBackSchemaLock(table_id: String, commit_id: String): Unit = {
    MetaLock.unlock(table_id, commit_id)
    deleteUndoLogByCommitId(UndoLogType.Schema.toString, table_id, commit_id)
  }

  private def rollBackAddedFile(table_id: String, commit_id: String): Unit = {
    deleteUndoLogByCommitId(UndoLogType.AddFile.toString, table_id, commit_id)
  }

  private def rollBackExpiredFile(table_id: String, commit_id: String): Unit = {
    deleteUndoLogByCommitId(UndoLogType.ExpireFile.toString, table_id, commit_id)
  }


}
