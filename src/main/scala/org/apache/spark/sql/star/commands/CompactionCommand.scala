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

package org.apache.spark.sql.star.commands

import com.engineplus.star.meta.MetaVersion
import org.apache.hadoop.fs.Path
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{Expression, PredicateHelper}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.execution.datasources.v2.merge.MergeDeltaParquetScan
import org.apache.spark.sql.star.catalog.StarLakeTableV2
import org.apache.spark.sql.star.exception.{MetaRerunException, StarLakeErrors}
import org.apache.spark.sql.star.sources.StarLakeSQLConf
import org.apache.spark.sql.star.utils.{DataFileInfo, PartitionInfo}
import org.apache.spark.sql.star.{BatchDataFileIndexV2, PartMergeTransactionCommit, SnapshotManagement, StarLakePartFileMerge, TransactionCommit}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.{Dataset, Row, SparkSession}

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks._


case class CompactionCommand(snapshotManagement: SnapshotManagement,
                             condition: Option[Expression],
                             force: Boolean,
                             mergeOperatorInfo: Map[String, String])
  extends PredicateHelper with Logging{


  /**
    * now：
    * 1. delta file num exceed threshold value
    * 2. this partition not been compacted, and last update time exceed threshold value
    */
  def filterPartitionNeedCompact(spark: SparkSession,
                                 force: Boolean,
                                 partitionInfo: PartitionInfo): Boolean = {
    val timestampLimit = System.currentTimeMillis() - spark.conf.get(StarLakeSQLConf.COMPACTION_TIME)

    if (force) {
      !partitionInfo.be_compacted
    } else {
      if (partitionInfo.delta_file_num >= spark.conf.get(StarLakeSQLConf.MAX_DELTA_FILE_NUM)) {
        true
      } else if (partitionInfo.last_update_timestamp <= timestampLimit
        && !partitionInfo.be_compacted) {
        true
      } else {
        false
      }
    }

  }

  def executeCompaction(spark: SparkSession, tc: TransactionCommit, files: Seq[DataFileInfo]): Unit = {
    val fileIndex = BatchDataFileIndexV2(spark, snapshotManagement, files)
    val table = StarLakeTableV2(
      spark,
      new Path(snapshotManagement.table_name),
      None,
      None,
      Option(fileIndex),
      Option(mergeOperatorInfo)
    )
    val option = new CaseInsensitiveStringMap(
      Map("basePath" -> tc.tableInfo.table_name, "isCompaction" -> "true"))

//    val compactDF = Dataset.ofRows(
//      spark,
//      DataSourceV2ScanRelation(
//        table,
//        table.newScanBuilder(option).build(),
//        table.schema().toAttributes
//      )
//    )
//
//    tc.setCommitType("compaction")
//    val newFiles = tc.writeFiles(compactDF)
//    tc.commit(newFiles, files)
//
//    logInfo("=========== Compaction Success!!! ===========")

    val scan = table.newScanBuilder(option).build()
    val newReadFiles = scan.asInstanceOf[MergeDeltaParquetScan].newFileIndex.getFileInfo(Nil)

    val compactDF = Dataset.ofRows(
      spark,
      DataSourceV2ScanRelation(
        table,
        scan,
        table.schema().toAttributes
      )
    )

    tc.setReadFiles(newReadFiles)
    tc.setCommitType("compaction")
    val newFiles = tc.writeFiles(compactDF)
    tc.commit(newFiles, newReadFiles)

    logInfo("=========== Compaction Success!!! ===========")
  }

//  def executePartFileCompaction(spark: SparkSession,
//                                pmtc: PartMergeTransactionCommit,
//                                files: Seq[DataFileInfo],
//                                commitFlag: Boolean): (Boolean, Seq[DataFileInfo]) = {
//    val fileIndex = BatchDataFileIndexV2(spark, snapshotManagement, files)
//    val table = StarLakeTableV2(
//      spark,
//      new Path(snapshotManagement.table_name),
//      None,
//      None,
//      Option(fileIndex),
//      Option(mergeOperatorInfo)
//    )
//    val option = new CaseInsensitiveStringMap(
//      Map("basePath" -> pmtc.tableInfo.table_name, "isCompaction" -> "true"))
//
//    val compactDF = Dataset.ofRows(
//      spark,
//      DataSourceV2ScanRelation(
//        table,
//        table.newScanBuilder(option).build(),
//        table.schema().toAttributes
//      )
//    )
//
//    pmtc.setReadFiles(files)
//    pmtc.setCommitType("compaction")
//
//    val newFiles = pmtc.writeFiles(compactDF)
//
//    //if part compaction failed before, it will not commit later
//    var flag = commitFlag
//    if(flag){
//      try {
//        pmtc.commit(newFiles, files)
//      } catch {
//        case e: MetaRerunException =>
//          if(e.getMessage.contains("deleted by another job")){
//            flag = false
//          }
//        case e: Exception => throw e
//      }
//
//    }
//
//    (flag, newFiles)
//
//  }

  //part compaction files one by one, and return remain files
//  def partMergeCompaction(sparkSession: SparkSession,
//                          groupAndSortedFiles: Iterable[Seq[DataFileInfo]]): Seq[DataFileInfo] = {
//    val limitMergeSize = 10
//
//    var currentVersion: Long = 0
//    var size: Long = 0
//    var notFinish = true
//    var commitFlag = true
//
//
//    var needMergeFiles = groupAndSortedFiles
//
//    while(notFinish){
//      val iter = needMergeFiles.head.iterator
//      breakable {
//        while(iter.hasNext){
//          val file = iter.next()
//          size += file.size
//          currentVersion = file.write_version
//          if(size > limitMergeSize){
//            snapshotManagement.withNewPartMergeTransaction(pmtc => {
//              val partFiles = needMergeFiles.flatMap(_.filter(_.write_version < currentVersion)).toSeq
//              val (flag, newFiles) = StarLakePartFileMerge.executePartFileCompaction(
//                sparkSession,
//                snapshotManagement,
//                pmtc,
//                partFiles,
//                mergeOperatorInfo,
//                commitFlag)
//
//              val notMergedFiles = needMergeFiles.flatMap(_.filter(_.write_version >= currentVersion)).toSeq
//              val newFilesChangeWriteVersion = newFiles.map(_.copy(write_version = 0))
//              needMergeFiles = (newFilesChangeWriteVersion ++ notMergedFiles)
//                .groupBy(_.file_bucket_id).values.map(m => m.sortBy(_.write_version))
//
//              size = 0
//              currentVersion = 0
//              commitFlag = flag
//            })
//            break
//          }
//        }
//      }
//
//      notFinish = false
//    }
//
//    needMergeFiles.flatten.toSeq
//  }

  def run(sparkSession: SparkSession): Seq[Row] = {
    //when condition is defined, only one partition need compaction,
    //else we will check whole table
    if (condition.isDefined) {
      val targetOnlyPredicates =
        splitConjunctivePredicates(condition.get)

      snapshotManagement.withNewTransaction(tc => {
        val files = tc.filterFiles(targetOnlyPredicates)

        //ensure only one partition execute compaction command
        val partitionSet = files.map(_.range_key).toSet
        if (partitionSet.isEmpty) {
          throw StarLakeErrors.partitionColumnNotFoundException(condition.get, 0)
        } else if (partitionSet.size > 1) {
          throw StarLakeErrors.partitionColumnNotFoundException(condition.get, partitionSet.size)
        }

        val range_value = partitionSet.head
        val table_id = tc.tableInfo.table_id
        val range_id = tc.snapshot.getPartitionInfoArray
          .filter(part => part.range_value.equals(range_value))
          .head.range_id

        val partitionInfo = MetaVersion.getSinglePartitionInfo(table_id, range_value, range_id)

//        val groupAndSortedFiles = files.map(m => {
//          //change write version to 0 when it is base file
//          if(m.is_base_file){
//            m.copy(write_version = 0)
//          }else{
//            m
//          }
//        }).groupBy(_.file_bucket_id).values.map(m => m.sortBy(_.write_version))

//        lazy val hasNoDeltaFile = if (force) {
//          false
//        } else {
//          groupAndSortedFiles.forall(_.size == 1)
//        }

//        val minimumDeltaFiles = sparkSession.sessionState.conf.getConf(StarLakeSQLConf.PART_MERGE_FILE_MINIMUM_NUM)
//        val maxFiles = groupAndSortedFiles.head.length

        lazy val hasNoDeltaFile = if (force) {
          false
        } else {
          files.groupBy(_.file_bucket_id).forall(_._2.size == 1)
        }
//
//        if (partitionInfo.be_compacted || hasNoDeltaFile) {
//          logInfo("== Compaction: This partition has been compacted or has no delta file.")
//        } else if(maxFiles < minimumDeltaFiles ) {
//          executeCompaction(sparkSession, tc, files)
//        }else{
//          //          val limitMergeSize = 10
//          //
//          //          var currentVersion: Long = 0
//          //          var size: Long = 0
//          //          var notFinish = true
//          //          var commitFlag = true
//          //
//          //
//          //          var needMergeFiles = groupAndSortedFiles
//          //
//          //          while(notFinish){
//          //            val iter = needMergeFiles.head.iterator
//          //            breakable {
//          //              while(iter.hasNext){
//          //                val file = iter.next()
//          //                size += file.size
//          //                currentVersion = file.write_version
//          //                if(size > limitMergeSize){
//          //                  snapshotManagement.withNewPartMergeTransaction(pmtc => {
//          //                    val partFiles = needMergeFiles.flatMap(_.filter(_.write_version < currentVersion)).toSeq
//          //
//          //                    val (flag, newFiles) = executePartFileCompaction(sparkSession, pmtc, partFiles, commitFlag)
//          //
//          //                    commitFlag = flag
//          //                    val notMergedFiles = needMergeFiles.flatMap(_.filter(_.write_version >= currentVersion)).toSeq
//          //                    val newFilesChangeWriteVersion = newFiles.map(_.copy(write_version = 0))
//          //                    needMergeFiles = (newFilesChangeWriteVersion ++ notMergedFiles)
//          //                      .groupBy(_.file_bucket_id).values.map(m => m.sortBy(_.write_version))
//          //
//          //                  })
//          //
//          //                  break()
//          //                }
//          //              }
//          //            }
//          //
//          //            notFinish = false
//          //          }
//
//
//          val remainFiles = partMergeCompaction(sparkSession, groupAndSortedFiles)
//          tc.setReadFiles(remainFiles)
//          executeCompaction(sparkSession, tc, remainFiles)
//        }

        if (partitionInfo.be_compacted || hasNoDeltaFile) {
          logInfo("== Compaction: This partition has been compacted or has no delta file.")
        } else {
          executeCompaction(sparkSession, tc, files)
        }

      })
    } else {

      val allInfo = MetaVersion.getAllPartitionInfo(snapshotManagement.getTableInfoOnly.table_id)
      val partitionsNeedCompact = allInfo
        .filter(filterPartitionNeedCompact(sparkSession, force, _))

      partitionsNeedCompact.foreach(part => {
        snapshotManagement.withNewTransaction(tc => {
          val files = tc.getCompactionPartitionFiles(part)
//          val groupAndSortedFiles = files.map(m => {
//            //change write version to 0 when it is base file
//            if(m.is_base_file){
//              m.copy(write_version = 0)
//            }else{
//              m
//            }
//          }).groupBy(_.file_bucket_id).values.map(m => m.sortBy(_.write_version))
//
//          val hasNoDeltaFile = if (force) {
//            false
//          } else {
//            groupAndSortedFiles.forall(_.size == 1)
//          }
//
//          val minimumDeltaFiles = sparkSession.sessionState.conf.getConf(StarLakeSQLConf.PART_MERGE_FILE_MINIMUM_NUM)
//          val maxFiles = groupAndSortedFiles.head.length
//
//          if (hasNoDeltaFile) {
//            logInfo(s"== Partition ${part.range_value} has no delta file.")
//          } else if(maxFiles < minimumDeltaFiles ) {
//            executeCompaction(sparkSession, tc, files)
//          }else{
//            val remainFiles = partMergeCompaction(sparkSession, groupAndSortedFiles)
//            tc.setReadFiles(remainFiles)
//            executeCompaction(sparkSession, tc, remainFiles)
//          }

          val hasNoDeltaFile = if (force) {
            false
          } else {
            files.groupBy(_.file_bucket_id).forall(_._2.size == 1)
          }
          if (hasNoDeltaFile) {
            logInfo(s"== Partition ${part.range_value} has no delta file.")
          } else {
            executeCompaction(sparkSession, tc, files)
          }
        })
      })


    }


    Seq.empty
  }


}