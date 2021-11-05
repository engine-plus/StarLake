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

import java.util.concurrent.{Executors, TimeUnit}

import com.engineplus.star.meta.MetaCommit.generateCommitIdToAddUndoLog
import com.engineplus.star.tables.StarTable
import org.apache.spark.sql.star.exception.MetaRerunException
import org.apache.spark.sql.star.test.StarLakeTestUtils
import org.apache.spark.sql.star.utils.{CommitOptions, DataFileInfo, MetaInfo, PartitionInfo}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{AnalysisException, DataFrame, QueryTest}

import scala.collection.mutable.ArrayBuffer


trait MetaCommitSuiteBase extends QueryTest
  with SharedSparkSession with StarLakeTestUtils {

  override def beforeAll(): Unit = {
    super.beforeAll()
    MetaTableManage.initDatabaseAndTables()
  }


  import testImplicits._

  def initTable(tablePath: String): Unit = {
    Seq(("a", 1), ("b", 2), ("c", 3)).toDF("key", "value")
      .write.partitionBy("key").format("star").mode("append")
      .save(tablePath)
  }

  def initHashTable(tablePath: String): Unit = {
    Seq(("a", 1, 1), ("b", 1, 2), ("c", 1, 3)).toDF("key", "hash", "value")
      .write.partitionBy("key")
      .option("hashPartitions", "hash")
      .option("hashBucketNum", "1")
      .format("star").mode("append")
      .save(tablePath)
  }

  def addDataFileInfo(key: String, num: Int): Seq[DataFileInfo] = {
    Seq(DataFileInfo(
      "addFile:" + key + "_" + num,
      MetaUtils.getPartitionMapFromKey(key),
      233,
      456,
      -1,
      true,
      "test_value"
    ))
  }

  def getNewPartInfoWithAddFile(partitionInfoArr: Array[PartitionInfo],
                                num: Int): Array[PartitionInfo] = {
    partitionInfoArr.map(m => {
      val files = DataOperation
        .getSinglePartitionDataInfo(m.table_id, m.range_id, m.range_value, m.read_version)
        .toArray
      m.copy(
        read_files = files,
        add_files = addDataFileInfo(m.range_value, num).toArray,
      )
    })
  }

  def getNewPartInfoWithAddAndExpireFile(partitionInfoArr: Array[PartitionInfo],
                                         num: Int): Array[PartitionInfo] = {
    partitionInfoArr.map(m => {
      val files = DataOperation
        .getSinglePartitionDataInfo(m.table_id, m.range_id, m.range_value, m.read_version)
        .toArray
      m.copy(
        read_files = files,
        add_files = addDataFileInfo(m.range_value, num).toArray,
        expire_files = files
      )
    })
  }


  def commitTest(commitType: String, changeSchema: Boolean): Unit = {
    test(s"$commitType commit, change schema: $changeSchema") {
      withTempDir(tmpDir => {
        val tableName = MetaUtils.modifyTableString(tmpDir.getCanonicalPath)
        initTable(tableName)

        val tableInfo = MetaVersion.getTableInfo(tableName)
        var partitionInfoArr = MetaVersion.getAllPartitionInfo(tableInfo.table_id)
        val newPartitionInfoArr = getNewPartInfoWithAddAndExpireFile(partitionInfoArr, 1)
        val metaInfo = MetaInfo(
          tableInfo,
          newPartitionInfoArr,
          CommitType(commitType))

        MetaCommit.doMetaCommit(metaInfo, changeSchema, CommitOptions(None, None))
        partitionInfoArr = MetaVersion.getAllPartitionInfo(tableInfo.table_id)
        assert(partitionInfoArr.map(_.read_version).forall(_ == 2))
        assert(partitionInfoArr.forall(m => {
          val fileName = "addFile:" + m.range_value + "_" + 1
          DataOperation
            .getSinglePartitionDataInfo(m.table_id, m.range_id, m.range_value, m.read_version)
            .head
            .file_path
            .equals(fileName)
        }))

      })
    }
  }


  def concurrentCommit(commitType: String, partitionNum: String, taskNum: Int, derange: Boolean): Unit = {
    test(s"concurrent $commitType commit, change schema: false - $partitionNum partition, derange: $derange") {
      withTempDir(tmpDir => {
        val tableName = MetaUtils.modifyTableString(tmpDir.getCanonicalPath)
        initTable(tableName)

        val tableInfo = MetaVersion.getTableInfo(tableName)
        var partitionInfoArr = MetaVersion.getAllPartitionInfo(tableInfo.table_id)

        val arrMetaInfo = new ArrayBuffer[MetaInfo]()

        for (i <- 0 until taskNum) {
          val newPartitionInfoArr = partitionNum match {
            case "single" => getNewPartInfoWithAddFile(Array(partitionInfoArr.head), i)
            case "multiple" =>
              if (derange) {
                //disturb order
                getNewPartInfoWithAddFile(partitionInfoArr, i)
                  .map(m => (m, scala.util.Random.nextInt(partitionInfoArr.length * 3)))
                  .sortBy(_._2)
                  .map(_._1)
              } else {
                getNewPartInfoWithAddFile(partitionInfoArr, i)
              }
            case _ => throw new Exception("Illegal partitionNum, it must be 'single' or 'multiple'")
          }
          //
          arrMetaInfo += MetaInfo(
            tableInfo,
            newPartitionInfoArr,
            CommitType(commitType)
          )
        }

        val pool = Executors.newFixedThreadPool(taskNum)

        for (i <- 0 until taskNum) {
          pool.execute(new Runnable {
            override def run(): Unit = {
              MetaCommit.doMetaCommit(arrMetaInfo(i), false, CommitOptions(None, None))
            }
          })

        }

        pool.shutdown()
        pool.awaitTermination(20, TimeUnit.MINUTES)


        partitionInfoArr = MetaVersion.getAllPartitionInfo(tableInfo.table_id)
        assert(partitionInfoArr.head.read_version == taskNum + 1)

      })
    }
  }


  def getNewPartitionDFSeq(num: Int): Seq[DataFrame] = {
    (0 until num).map(i => {
      Seq(("d", 1, i)).toDF("key", "hash", "value")
    })
  }


}

class MetaCommitSuite extends MetaCommitSuiteBase {

  Seq("simple", "delta", "compaction")
    .foreach(commitTest(_, false))

  Seq("delta")
    .foreach(t => {
      Seq("single", "multiple")
        .foreach(f => {
          if (f.equals("single")) {
            concurrentCommit(t, f, 5, false)
          } else if (f.equals("multiple")) {
            concurrentCommit(t, f, 5, false)
            concurrentCommit(t, f, 3, true)
          }
        })
    })


  test("Committing state will roll back when timeout") {
    withTempDir(tmpDir => {
      val tableName = MetaUtils.modifyTableString(tmpDir.getCanonicalPath)
      initTable(tableName)

      val tableInfo = MetaVersion.getTableInfo(tableName)
      var partitionInfoArr = MetaVersion.getAllPartitionInfo(tableInfo.table_id)
      val newPartitionInfoArr1 = getNewPartInfoWithAddAndExpireFile(partitionInfoArr, 1)
      val metaInfo1 = MetaInfo(
        tableInfo,
        newPartitionInfoArr1,
        CommitType("simple"))

      val oldReadVersion = partitionInfoArr.map(_.read_version).max

      val commit_id = generateCommitIdToAddUndoLog(
        metaInfo1.table_info.table_name,
        metaInfo1.table_info.table_id,
        "",
        -1L)
      //only get partition lock
      val newMetaInfo = MetaCommit.takePartitionsWriteLock(metaInfo1, commit_id)
      val newMetaInfo1 = MetaCommit.updatePartitionInfoAndGetNewMetaInfo(newMetaInfo)
      assert(newMetaInfo1.partitionInfoArray.map(_.pre_write_version).max == oldReadVersion + 1)


      //new task will commit successful anyway
      val newPartitionInfoArr2 = getNewPartInfoWithAddAndExpireFile(partitionInfoArr, 2)
      val metaInfo2 = MetaInfo(
        tableInfo,
        newPartitionInfoArr2,
        CommitType("simple"))
      MetaCommit.doMetaCommit(metaInfo2, false, CommitOptions(None, None))

      partitionInfoArr = MetaVersion.getAllPartitionInfo(tableInfo.table_id)
      assert(
        partitionInfoArr.map(_.read_version).max == oldReadVersion + 1 &&
          partitionInfoArr.forall(m => {
            val fileName = "addFile:" + m.range_value + "_" + 2
            DataOperation
              .getSinglePartitionDataInfo(m.table_id, m.range_id, m.range_value, m.read_version)
              .head
              .file_path
              .equals(fileName)
          })

      )

    })
  }


  test("check files conflict - files change while commit will throw MetaRerunException") {
    withTempDir(tmpDir => {
      val tableName = MetaUtils.modifyTableString(tmpDir.getCanonicalPath)
      initTable(tableName)

      val tableInfo = MetaVersion.getTableInfo(tableName)
      val partitionInfoArr = MetaVersion.getAllPartitionInfo(tableInfo.table_id)
      val newPartitionInfoArr1 = getNewPartInfoWithAddAndExpireFile(partitionInfoArr, 1)
      val newPartitionInfoArr2 = getNewPartInfoWithAddAndExpireFile(partitionInfoArr, 2)
      val metaInfo1 = MetaInfo(
        tableInfo,
        newPartitionInfoArr1,
        CommitType("simple"))
      val metaInfo2 = MetaInfo(
        tableInfo,
        newPartitionInfoArr2,
        CommitType("simple"))

      MetaCommit.doMetaCommit(metaInfo1, false, CommitOptions(None, None))

      val e = intercept[MetaRerunException](
        MetaCommit.doMetaCommit(metaInfo2, false, CommitOptions(None, None)))
      assert(e.getMessage.contains("Another job added file"))

    })
  }


  test("check files conflict - can't delete file twice") {
    withTempDir(tmpDir => {
      val tableName = MetaUtils.modifyTableString(tmpDir.getCanonicalPath)
      initTable(tableName)

      val tableInfo = MetaVersion.getTableInfo(tableName)
      val partitionInfoArr = MetaVersion.getAllPartitionInfo(tableInfo.table_id)
      val newPartitionInfoArr1 = getNewPartInfoWithAddAndExpireFile(partitionInfoArr, 1)
      val newPartitionInfoArr2 = getNewPartInfoWithAddAndExpireFile(partitionInfoArr, 2)
      val metaInfo1 = MetaInfo(
        tableInfo,
        newPartitionInfoArr1,
        CommitType("compaction"))
      val metaInfo2 = MetaInfo(
        tableInfo,
        newPartitionInfoArr2,
        CommitType("compaction"))

      MetaCommit.doMetaCommit(metaInfo1, false, CommitOptions(None, None))

      val e = intercept[MetaRerunException](
        MetaCommit.doMetaCommit(metaInfo2, false, CommitOptions(None, None)))
      assert(e.getMessage.contains("deleted by another job during write_version="))

    })
  }


  test("take schema lock concurrently") {
    withTempDir(tmpDir => {
      val tableName = MetaUtils.modifyTableString(tmpDir.getCanonicalPath)
      initTable(tableName)

      val tableInfo = MetaVersion.getTableInfo(tableName)
      val partitionInfoArr = MetaVersion.getAllPartitionInfo(tableInfo.table_id)


      val newPartitionInfoArr1 = getNewPartInfoWithAddFile(partitionInfoArr, 1)
      val metaInfo1 = MetaInfo(
        tableInfo,
        newPartitionInfoArr1,
        CommitType("delta"),
        "commitId1")

      val newSchema2 = new StructType()
        .add("key", "string")
        .add("add_col2", "string")
        .add("value", "integer").json
      val tableInfo2 = tableInfo.copy(table_schema = newSchema2)
      val newPartitionInfoArr2 = getNewPartInfoWithAddFile(partitionInfoArr, 2)
      val metaInfo2 = MetaInfo(
        tableInfo2,
        newPartitionInfoArr2,
        CommitType("delta"))


      MetaCommit.takeSchemaLock(metaInfo1)
      MetaCommit.doMetaCommit(metaInfo2, true, CommitOptions(None, None))
      val currentTableInfo = MetaVersion.getTableInfo(tableName)
      assert(currentTableInfo.schema_version == tableInfo.schema_version + 1 &&
        currentTableInfo.table_schema.equals(newSchema2))

      val newSchema3 = new StructType()
        .add("key", "string")
        .add("add_col3", "string")
        .add("value", "integer").json
      val tableInfo3 = tableInfo.copy(table_schema = newSchema3)
      val newPartitionInfoArr3 = getNewPartInfoWithAddFile(partitionInfoArr, 3)
      val metaInfo3 = MetaInfo(
        tableInfo3,
        newPartitionInfoArr3,
        CommitType("delta"))

      val e = intercept[AnalysisException] {
        MetaCommit.doMetaCommit(metaInfo3, true, CommitOptions(None, None))
      }

      assert(e.getMessage().contains("Schema has been changed for table"))
    })
  }


  test("create range partition concurrently") {
    withTempDir(tmpDir => {
      val tableName = MetaUtils.modifyTableString(tmpDir.getCanonicalPath)
      initHashTable(tableName)

      val taskNum = 5
      val dfArr = getNewPartitionDFSeq(taskNum)

      val table = StarTable.forPath(tableName)


      val pool = Executors.newFixedThreadPool(taskNum)

      for (i <- 0 until taskNum) {
        pool.execute(new Runnable {
          override def run(): Unit = {
            table.upsert(dfArr(i))
          }
        })

      }

      pool.shutdown()
      pool.awaitTermination(20, TimeUnit.MINUTES)


      val tableInfo = MetaVersion.getTableInfo(tableName)
      val partitionInfoArr = MetaVersion.getAllPartitionInfo(tableInfo.table_id)
      assert(partitionInfoArr.filter(_.range_value.equals("key=d")).head.read_version == taskNum)
    })


  }


}
