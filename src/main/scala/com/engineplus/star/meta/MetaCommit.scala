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

import com.engineplus.star.meta.Redo._
import com.engineplus.star.meta.RollBack._
import com.engineplus.star.meta.UndoLog._
import org.apache.spark.internal.Logging
import org.apache.spark.sql.star.exception._
import org.apache.spark.sql.star.utils.{MetaInfo, PartitionInfo, commitStateInfo}

import scala.collection.mutable.ArrayBuffer

object MetaCommit extends Logging{
  //meta commit process
  def doMetaCommit(meta_info: MetaInfo, changeSchema: Boolean, times: Int = 0): Unit = {
    //add commit type undo log, generate commit_id
    val commit_id = generateCommitIdToAddUndoLog(
      meta_info.table_info.table_name,
      meta_info.table_info.table_id,
      meta_info.query_id,
      meta_info.batch_id)

    try {
      //hold all needed partition lock
      var new_meta_info = takePartitionsWriteLock(meta_info, commit_id)

      //if table schema will be changed in this commit, hold schema lock
      if (changeSchema) {
        takeSchemaLock(new_meta_info)
      }

      //update write version, compaction info, delta file num
      new_meta_info = updatePartitionInfoAndGetNewMetaInfo(new_meta_info)

      //check files conflict
      fileConflictDetection(new_meta_info)

      //add file changing info to table undo_log
      addDataInfo(new_meta_info)

      //change commit state to redo, roll back is forbidden in this time, this commit must be success
      markSelfCommitSuccess(new_meta_info.table_info.table_id, new_meta_info.commit_id)

      //change schema, release schema lock, and delete corresponding undo log
      if (changeSchema) {
        updateSchema(new_meta_info)
      }

      //add/update files info to table data_info, release partition lock and delete undo log
      commitMetaInfo(new_meta_info, changeSchema)

    } catch {
      case e: MetaRetryException =>
        e.printStackTrace()
        logInfo("!!!!!!! Error Retry commit id: " + commit_id)
        rollBackUpdate(meta_info, commit_id, changeSchema)
        if (times < MetaUtils.MAX_COMMIT_ATTEMPTS) {
          doMetaCommit(meta_info, changeSchema, times + 1)
        } else {
          throw StarLakeErrors.commitFailedReachLimit(
            meta_info.table_info.table_name,
            commit_id,
            MetaUtils.MAX_COMMIT_ATTEMPTS)
        }

      case e: Throwable =>
        rollBackUpdate(meta_info, commit_id, changeSchema)
        throw e
    }

  }


  //add commit type undo log, generate commit id
  def generateCommitIdToAddUndoLog(table_name: String,
                                   table_id: String,
                                   queryId: String,
                                   batchId: Long): String = {
    val commit_id = "commit_" + java.util.UUID.randomUUID().toString
    logInfo(s"{{{{{{commit: $commit_id begin!!!}}}}}}")
    val timestamp = System.currentTimeMillis()
    if (!addCommitUndoLog(table_name, table_id, commit_id, timestamp, queryId, batchId)) {
      generateCommitIdToAddUndoLog(table_name, table_id, queryId, batchId)
    } else {
      commit_id
    }
  }


  def isCommitTimeout(timestamp: Long): Boolean = {
    val timeout = System.currentTimeMillis() - timestamp
    if (timeout > MetaUtils.COMMIT_TIMEOUT) {
      true
    } else {
      false
    }
  }

  def getCommitState(table_name: String,
                     table_id: String,
                     commit_id: String,
                     time_limit: Long = MetaUtils.COMMIT_TIMEOUT): commitStateInfo = {
    val (last_timestamp, tag) = getCommitTimestampAndTag(UndoLogType.Commit.toString, table_id, commit_id)
    if (last_timestamp < 1) {
      commitStateInfo(CommitState.Clean, table_name, table_id, commit_id, tag, last_timestamp)
    } else {
      val timeout = System.currentTimeMillis() - last_timestamp

      //not time out
      if (timeout < time_limit) {
        if (tag == 0) {
          commitStateInfo(CommitState.Committing, table_name, table_id, commit_id, tag, last_timestamp)
        }
        else if (tag == -1) {
          commitStateInfo(CommitState.Redoing, table_name, table_id, commit_id, tag, last_timestamp)
        }
        else {
          commitStateInfo(CommitState.RollBacking, table_name, table_id, commit_id, tag, last_timestamp)
        }
      }
      else {
        if (tag == 0) {
          commitStateInfo(CommitState.CommitTimeout, table_name, table_id, commit_id, tag, last_timestamp)
        }
        else if (tag == -1) {
          commitStateInfo(CommitState.RedoTimeout, table_name, table_id, commit_id, tag, last_timestamp)
        }
        else {
          commitStateInfo(CommitState.RollBackTimeout, table_name, table_id, commit_id, tag, last_timestamp)
        }
      }
    }
  }


  def lockSinglePartition(partition_info: PartitionInfo,
                          commit_id: String,
                          times: Int = 0): Unit = {

    //update timestamp, if timeout, exit the loop directly and rollback, retry after a random sleep
    updateCommitTimestamp(partition_info.table_id, commit_id)

    //lock
    val (lock_flag, last_commit_id) = MetaLock.lock(partition_info.range_id, commit_id)

    if (!lock_flag) {
      //if lock failed, it prove that another job is committing, check its state
      val state_info = getCommitState(partition_info.table_name, partition_info.table_id, last_commit_id)

      logInfo(s"Another job's commit state is ${state_info.state.toString} ~~")

      state_info.state match {
        //wait a moment, increase count and retry
        case CommitState.Committing =>
          if (times < MetaUtils.GET_LOCK_MAX_ATTEMPTS) {
            TimeUnit.SECONDS.sleep(MetaUtils.WAIT_LOCK_INTERVAL)
            lockSinglePartition(partition_info, commit_id, times + 1)
          } else {
            //it may has deadlock
            throw new MetaNonfatalException("There may have deadlock.")
          }
        //wait a moment and retry
        case CommitState.RollBacking | CommitState.Redoing =>
          TimeUnit.SECONDS.sleep(MetaUtils.WAIT_LOCK_INTERVAL)
          lockSinglePartition(partition_info, commit_id, times)

        //rollback and retry
        case CommitState.CommitTimeout | CommitState.RollBackTimeout =>
          rollBackCommit(state_info.table_id, state_info.commit_id, state_info.tag, state_info.timestamp)
          lockSinglePartition(partition_info, commit_id)

        //clean
        case CommitState.Clean =>
          cleanRollBackCommit(state_info.table_id, state_info.commit_id, partition_info.range_id)
          lockSinglePartition(partition_info, commit_id)

        //redo and retry
        case CommitState.RedoTimeout =>
          redoCommit(state_info.table_name, state_info.table_id, state_info.commit_id)
          lockSinglePartition(partition_info, commit_id)
      }
    }
  }

  def takePartitionsWriteLock(meta_info: MetaInfo, commit_id: String, times: Int = 0): MetaInfo = {
    val new_partition_info_arr_buf = new ArrayBuffer[PartitionInfo]()
    val partition_info_itr = meta_info.partitionInfoArray.iterator

    try {
      while (partition_info_itr.hasNext) {
        val partition_info = partition_info_itr.next()

        var rangeId = ""
        var changed = false

        try {
          if (!MetaVersion.isPartitionExists(
            partition_info.table_id,
            partition_info.range_value,
            partition_info.range_id,
            commit_id)) {
            MetaVersion.addPartition(
              partition_info.table_id,
              partition_info.table_name,
              partition_info.range_id,
              partition_info.range_value)
          }
        } catch {
          //create partition concurrently
          case e: MetaRerunException if (e.getMessage.contains("another job drop and create a newer one.")
            && partition_info.read_version == 1
            && partition_info.pre_write_version == 1) =>
            //the latter committer can use exist range_id
            val (partitionExists, now_range_id) =
              MetaVersion.getPartitionId(partition_info.table_id, partition_info.range_value)
            if (partitionExists) {
              rangeId = now_range_id
              changed = true
            } else {
              throw e
            }

          case e: MetaException if (e.getMessage.contains("Failed to add partition version for table:")
            && partition_info.read_version == 1
            && partition_info.pre_write_version == 1) =>
            //the latter committer can use exist range_id
            val (partitionExists, now_range_id) =
              MetaVersion.getPartitionId(partition_info.table_id, partition_info.range_value)
            if (partitionExists) {
              rangeId = now_range_id
              changed = true
            } else {
              throw e
            }

          case e: Exception => throw e

        }

        val newPartitionInfo = if (changed) {
          partition_info.copy(range_id = rangeId)
        } else {
          partition_info
        }

        addPartitionUndoLog(
          newPartitionInfo.table_name,
          newPartitionInfo.range_value,
          newPartitionInfo.table_id,
          newPartitionInfo.range_id,
          commit_id,
          newPartitionInfo.delta_file_num,
          newPartitionInfo.be_compacted)

        lockSinglePartition(newPartitionInfo, commit_id)

        new_partition_info_arr_buf += newPartitionInfo
      }

      meta_info.copy(
        partitionInfoArray = new_partition_info_arr_buf.toArray,
        commit_id = commit_id)
    } catch {
      case e: MetaNonfatalException =>
        if (times < MetaUtils.GET_LOCK_MAX_ATTEMPTS) {
          //it may has deadlock, release all hold locks
          rollBackPartitionLock(meta_info.table_info.table_id, commit_id)
          updateCommitTimestamp(meta_info.table_info.table_id, commit_id)
          logInfo("  too many jobs require to update,sleep a while ~~~~")
          TimeUnit.SECONDS.sleep(scala.util.Random.nextInt((times + 1) * MetaUtils.RETRY_LOCK_INTERVAL))

          //increase count and retry
          takePartitionsWriteLock(meta_info, commit_id, times + 1)
        } else {
          throw MetaRetryErrors.tooManyCommitException()
        }

      case e: Throwable => throw e
    }

  }


  def lockSchema(meta_info: MetaInfo): Unit = {
    val (lock_flag, last_commit_id) = MetaLock.lock(meta_info.table_info.table_id, meta_info.commit_id)
    if (!lock_flag) {
      //get state of last commit
      val state_info = getCommitState(
        meta_info.table_info.table_name,
        meta_info.table_info.table_id,
        last_commit_id)

      logInfo(s"Another job's commit state is ${state_info.state.toString} ~~")

      state_info.state match {
        case CommitState.Committing | CommitState.RollBacking | CommitState.Redoing =>
          TimeUnit.SECONDS.sleep(MetaUtils.WAIT_LOCK_INTERVAL)

        case CommitState.CommitTimeout | CommitState.RollBackTimeout =>
          rollBackCommit(state_info.table_id, state_info.commit_id, state_info.tag, state_info.timestamp)

        case CommitState.Clean =>
          cleanRollBackCommit(state_info.table_id, state_info.commit_id, state_info.table_id)

        case CommitState.RedoTimeout =>
          redoCommit(state_info.table_name, state_info.table_id, state_info.commit_id)
          throw StarLakeErrors.schemaChangedException(state_info.table_name)
      }
      //retry
      lockSchema(meta_info)
    }
  }

  def takeSchemaLock(meta_info: MetaInfo): Unit = {
    val schema_write_version = meta_info.table_info.schema_version + 1
    addSchemaUndoLog(
      meta_info.table_info.table_name,
      meta_info.table_info.table_id,
      meta_info.commit_id,
      schema_write_version,
      meta_info.table_info.table_schema,
      MetaUtils.toCassandraSetting(meta_info.table_info.configuration))

    //lock
    lockSchema(meta_info)

    //if schema had been changed, throw exception directly
    val current_schema_version = MetaVersion.getTableInfo(meta_info.table_info.table_name).schema_version
    if (meta_info.table_info.schema_version != current_schema_version) {
      throw StarLakeErrors.schemaChangedException(meta_info.table_info.table_name)
    }
  }

  def addDataInfo(meta_info: MetaInfo): Unit = {
    val table_name = meta_info.table_info.table_name
    val table_id = meta_info.table_info.table_id
    val commit_id = meta_info.commit_id

    //update timestamp to avoid committing timeout
    updateCommitTimestamp(table_id, commit_id)

    //add all files change undo log
    for (partition_info <- meta_info.partitionInfoArray) {
      val range_id = partition_info.range_id
      val write_version = getWriteVersionFromMetaInfo(meta_info, partition_info)
      for (file <- partition_info.expire_files) {
        expireFileUndoLog(
          table_name,
          table_id,
          range_id,
          commit_id,
          file.file_path,
          write_version,
          file.modification_time)
      }
      for (file <- partition_info.add_files) {
        addFileUndoLog(
          table_name,
          table_id,
          range_id,
          commit_id,
          file.file_path,
          write_version,
          file.size,
          file.modification_time,
          file.file_exist_cols,
          file.is_base_file)
      }
    }
  }

  def updateSchema(meta_info: MetaInfo): Unit = {
    val table_name = meta_info.table_info.table_name
    val table_id = meta_info.table_info.table_id
    val commit_id = meta_info.commit_id

    val new_read_version = meta_info.table_info.schema_version + 1

    //schema may be too long to store in a cassandra field(64KB),in this case the schema value will be split,
    //and the value here is a collection of index in table fragment_value
    val schema_index = getUndoLogInfo(UndoLogType.Schema.toString, table_id, commit_id).head.table_schema
    MetaVersion.updateTableSchema(
      table_name,
      table_id,
      commit_id,
      schema_index,
      meta_info.table_info.configuration,
      new_read_version)

    MetaLock.unlock(table_id, commit_id)
    deleteUndoLogByCommitId(UndoLogType.Schema.toString, table_id, commit_id)
  }


  def commitMetaInfo(meta_info: MetaInfo,
                     changeSchema: Boolean): Unit = {
    val table_name = meta_info.table_info.table_name
    val table_id = meta_info.table_info.table_id
    val commit_id = meta_info.commit_id

    for (partition_info <- meta_info.partitionInfoArray) {
      val range_value = partition_info.range_value
      val range_id = partition_info.range_id
      val write_version = getWriteVersionFromMetaInfo(meta_info, partition_info)

      for (file <- partition_info.expire_files) {
        DataOperation.deleteExpireDataFile(
          table_id,
          range_id,
          file.file_path,
          write_version,
          commit_id,
          file.modification_time)
      }
      for (file <- partition_info.add_files) {
        DataOperation.addNewDataFile(
          table_id,
          range_id,
          file.file_path,
          write_version,
          commit_id,
          file.size,
          file.modification_time,
          file.file_exist_cols,
          file.is_base_file)
      }

      MetaVersion.updatePartitionReadVersion(
        table_id,
        range_id,
        table_name,
        range_value,
        write_version,
        commit_id,
        partition_info.delta_file_num,
        partition_info.be_compacted)

      MetaLock.unlock(partition_info.range_id, commit_id)
    }
    deleteUndoLogByCommitId(UndoLogType.AddFile.toString, table_id, commit_id)
    deleteUndoLogByCommitId(UndoLogType.ExpireFile.toString, table_id, commit_id)
    deleteUndoLogByCommitId(UndoLogType.Partition.toString, table_id, commit_id)

    //if it is streaming job, streaming info should be updated
    if(meta_info.query_id.nonEmpty && meta_info.batch_id >= 0){
      StreamingRecord.updateStreamingInfo(table_id, meta_info.query_id, meta_info.batch_id, System.currentTimeMillis())
    }

    deleteUndoLogByCommitId(UndoLogType.Commit.toString, table_id, commit_id)
    logInfo(s"{{{{{{commit: $commit_id success!!!}}}}}}")
  }


  //before write files info to table data_info, the new write_version should be got from metaInfo
  def getWriteVersionFromMetaInfo(meta_info: MetaInfo, partition_info: PartitionInfo): Long = {
    var write_version = -1L
    for (meta_partition_info <- meta_info.partitionInfoArray) {
      if (meta_partition_info.range_value.equals(partition_info.range_value)) {
        write_version = meta_partition_info.pre_write_version
      }
    }

    if (write_version == -1) {
      val partitions_arr_buf = new ArrayBuffer[String]()
      for (partition_info <- meta_info.partitionInfoArray) {
        partitions_arr_buf += partition_info.range_value
      }
      throw StarLakeErrors.getWriteVersionError(
        meta_info.table_info.table_name,
        partition_info.range_value,
        partitions_arr_buf.toArray.mkString(","),
        meta_info.commit_id
      )
    }
    write_version
  }

  //check files conflict
  def fileConflictDetection(meta_info: MetaInfo): Unit = {
    val update_file_info_arr = meta_info.partitionInfoArray
    meta_info.commit_type match {
      case DeltaCommit => //files conflict checking is not required when committing delta files
      case CompactionCommit =>
        //when committing compaction job, it just need to check that the read files have not been deleted
        DataOperation.checkDataInfo(meta_info.commit_id, update_file_info_arr, false, true)
      case _ =>
        DataOperation.checkDataInfo(meta_info.commit_id, update_file_info_arr, true, true)
    }
  }


  //check and redo timeout commits before read
  def checkAndRedoCommit(table_id: String): Unit = {
    val limit_timestamp = System.currentTimeMillis() - MetaUtils.COMMIT_TIMEOUT
    val logInfo = UndoLog.getTimeoutUndoLogInfo(UndoLogType.Commit.toString, table_id, limit_timestamp)

    var flag = true
    logInfo.foreach(log => {
      if (log.tag == -1) {
        if (!Redo.redoCommit(log.table_name, log.table_id, log.commit_id)) {
          flag = false
        }
      } else {
        RollBack.rollBackCommit(log.table_id, log.commit_id, log.tag, log.timestamp)
      }
    })

    if (!flag) {
      checkAndRedoCommit(table_id)
    }

  }

  def cleanUndoLog(table_id: String): Unit = {
    checkAndRedoCommit(table_id)
    val limit_timestamp = System.currentTimeMillis() - MetaUtils.UNDO_LOG_TIMEOUT

    val log_info_seq = UndoLogType.getAllType
      .map(UndoLog.getTimeoutUndoLogInfo(_, table_id, limit_timestamp))
      .flatMap(_.toSeq)

    val commit_log_info_seq = log_info_seq.filter(_.commit_type.equalsIgnoreCase(UndoLogType.Commit.toString))
    val other_log_info_seq = log_info_seq.filter(!_.commit_type.equalsIgnoreCase(UndoLogType.Commit.toString))

    //delete undo log directly if table is not exists
    commit_log_info_seq
      .filter(log => !MetaVersion.isTableIdExists(log.table_name, log.table_id))
      .foreach(log => deleteUndoLogByCommitId(UndoLogType.Commit.toString, table_id, log.commit_id))

    //if a commit_id don't has corresponding undo log with commit type, it should be clean
    val skip_commit_id =
      other_log_info_seq
        .map(_.commit_id).toSet
        .filter(hasCommitTypeLog(table_id, _))

    other_log_info_seq
      .filter(log => !skip_commit_id.contains(log.commit_id))
      .foreach(log => {
        log.commit_type match {
          case t: String if t.equalsIgnoreCase(UndoLogType.Schema.toString) =>
            MetaLock.unlock(log.table_id, log.commit_id)
            deleteUndoLogByCommitId(UndoLogType.Schema.toString, table_id, log.commit_id)
          case t: String if t.equalsIgnoreCase(UndoLogType.Partition.toString) =>
            MetaLock.unlock(log.range_id, log.commit_id)
            deleteUndoLogByCommitId(UndoLogType.Partition.toString, table_id, log.commit_id)
          case t: String if t.equalsIgnoreCase(UndoLogType.AddFile.toString) =>
            deleteUndoLogByCommitId(UndoLogType.AddFile.toString, table_id, log.commit_id)
          case t: String if t.equalsIgnoreCase(UndoLogType.ExpireFile.toString) =>
            deleteUndoLogByCommitId(UndoLogType.ExpireFile.toString, table_id, log.commit_id)
          case _ => StarLakeErrors.unknownUndoLogTypeException(_)
        }
      })

    //cleanup the timeout streaming info
    val streaming_expire_timestamp = System.currentTimeMillis() - MetaUtils.STREAMING_INFO_TIMEOUT
    StreamingRecord.deleteStreamingInfoByTimestamp(table_id, streaming_expire_timestamp)

  }


  def updatePartitionInfoAndGetNewMetaInfo(meta_info: MetaInfo): MetaInfo = {
    val new_partition_info_arr_buf = new ArrayBuffer[PartitionInfo]()
    val partition_info_itr = meta_info.partitionInfoArray.iterator
    val commit_id = meta_info.commit_id


    while (partition_info_itr.hasNext) {
      val partition_info = partition_info_itr.next()


      //update the write_version/be_compacted/delta_file_num info in partition undo log
      val currentPartitionInfo = MetaVersion
        .getSinglePartitionInfo(partition_info.table_id, partition_info.range_value, partition_info.range_id)
      val write_version = currentPartitionInfo.read_version + 1
      val versionDiff = currentPartitionInfo.read_version - partition_info.read_version
      val (compactInfo, deltaFileNum) = meta_info.commit_type match {
        case CompactionCommit =>
          if (versionDiff > 0) {
            (currentPartitionInfo.be_compacted, versionDiff.toInt)
          } else if (versionDiff == 0) {
            (partition_info.be_compacted, 0)
          } else {
            throw StarLakeErrors.readVersionIllegalException(
              currentPartitionInfo.read_version,
              partition_info.read_version)
          }
        case _ => (partition_info.be_compacted, currentPartitionInfo.delta_file_num + 1)
      }
      UndoLog.updatePartitionLogInfo(
        partition_info.table_id,
        commit_id,
        partition_info.range_id,
        write_version,
        compactInfo,
        deltaFileNum)

      new_partition_info_arr_buf += partition_info.copy(
        pre_write_version = write_version,
        be_compacted = compactInfo,
        delta_file_num = deltaFileNum)
    }

    meta_info.copy(partitionInfoArray = new_partition_info_arr_buf.toArray)


  }


}
