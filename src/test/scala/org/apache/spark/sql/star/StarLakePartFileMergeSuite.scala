package org.apache.spark.sql.star

import com.engineplus.star.tables.StarTable
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.star.sources.StarLakeSQLConf
import org.apache.spark.sql.star.test.{MergeOpInt, MergeOpString, MergeOpString02, StarLakeTestUtils}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.test.SharedSparkSession

class StarLakePartFileMergeSuite extends QueryTest
  with SharedSparkSession with StarLakeTestUtils {
  import testImplicits._

  test("simple part merge when there are to many delta files"){
    withTempDir(dir => {
      withSQLConf(StarLakeSQLConf.PART_MERGE_COMPACTION_COMMIT_ENABLE.key -> "true",
        StarLakeSQLConf.PART_MERGE_FILE_SIZE_FACTOR.key -> "0.0000001",
        StarLakeSQLConf.PART_MERGE_FILE_MINIMUM_NUM.key -> "3"){

        val tablePath = dir.getCanonicalPath

        val data1 = Seq(("range1","hash1","value1"),("range1","hash2","value2"),("range1","hash3","value3"))
          .toDF("range", "hash", "value")
        val data2 = Seq(("range1","hash1","value1"),("range1","hash2","value2"),("range1","hash3","value3"))
          .toDF("range", "hash", "value")
        val data3 = Seq(("range1","hash1","value1"),("range1","hash2","value21"),("range1","hash3","value31"))
          .toDF("range", "hash", "value")
        val data4 = Seq(("range1","hash1","value1"),("range1","hash2","value2"),("range1","hash3","value3"))
          .toDF("range", "hash", "value")
        val data5 = Seq(("range1","hash1","value1"),("range1","hash2","value22"),("range1","hash3","value33"))
          .toDF("range", "hash", "value")

        data1.write
          .mode("overwrite")
          .format("star")
          .option("rangePartitions", "range")
          .option("hashPartitions", "hash")
          .option("hashBucketNum", "1")
          .save(tablePath)

        val table = StarTable.forPath(tablePath)
        table.upsert(data2)
        table.upsert(data3)
        table.upsert(data4)
        table.upsert(data5)

        val snapshotManagement = SnapshotManagement(tablePath)
        var partitionInfo = snapshotManagement.snapshot.getPartitionInfoArray.head
        val oriDeltaNum = partitionInfo.delta_file_num
        val oriReadVersion = partitionInfo.read_version

        checkAnswer(table.toDF.select("range","hash", "value"),
          Seq(("range1","hash1","value1"),("range1","hash2","value22"),("range1","hash3","value33"))
            .toDF("range", "hash", "value"))
        partitionInfo = snapshotManagement.updateSnapshot().getPartitionInfoArray.head

        assert(partitionInfo.delta_file_num ==
          (oriDeltaNum % spark.sessionState.conf.getConf(StarLakeSQLConf.PART_MERGE_FILE_MINIMUM_NUM) + 1))

        assert(partitionInfo.read_version == oriReadVersion + 1)
      }


    })
  }

  test("part merge with merge operator"){
    withTempDir(dir => {
      withSQLConf(StarLakeSQLConf.PART_MERGE_COMPACTION_COMMIT_ENABLE.key -> "true",
        StarLakeSQLConf.PART_MERGE_FILE_SIZE_FACTOR.key -> "0.0000001",
        StarLakeSQLConf.PART_MERGE_FILE_MINIMUM_NUM.key -> "2"){

        val tablePath = dir.getCanonicalPath

        val data1 = Seq(("range1","hash1","a1",1),("range1","hash2","a2",2),("range1","hash3","a3",3))
          .toDF("range", "hash", "value1", "value2")
        val data2 = Seq(("range1","hash1","b1",11),("range1","hash2","b2",22),("range1","hash3","b3",33))
          .toDF("range", "hash", "value1", "value2")
        val data3 = Seq(("range1","hash1","c1",111),("range1","hash2","c2",222),("range1","hash3","c3",333))
          .toDF("range", "hash", "value1", "value2")
        val data4 = Seq(("range1","hash1","d1",1111),("range1","hash2","d2",2222),("range1","hash3","d3",3333))
          .toDF("range", "hash", "value1", "value2")
        val data5 = Seq(("range1","hash1","e1",11111),("range1","hash2","e2",22222),("range1","hash3","e3",33333))
          .toDF("range", "hash", "value1", "value2")

        data1.write
          .mode("overwrite")
          .format("star")
          .option("rangePartitions", "range")
          .option("hashPartitions", "hash")
          .option("hashBucketNum", "1")
          .save(tablePath)

        val table = StarTable.forPath(tablePath)
        table.upsert(data2)
        table.upsert(data3)
        table.upsert(data4)
        table.upsert(data5)

        val snapshotManagement = SnapshotManagement(tablePath)
        var partitionInfo = snapshotManagement.snapshot.getPartitionInfoArray.head
        val oriDeltaNum = partitionInfo.delta_file_num
        val oriReadVersion = partitionInfo.read_version


        new MergeOpString02().register(spark, "stringOp")
        new MergeOpInt().register(spark, "intOp")

        checkAnswer(table.toDF.select(
          col("range"),
          col("hash"),
          expr("stringOP(value1) as value1"),
          expr("intOp(value2) as value2")),
          Seq(
            ("range1","hash1","a1;b1;c1;d1;e1",12345),
            ("range1","hash2","a2;b2;c2;d2;e2",24690),
            ("range1","hash3","a3;b3;c3;d3;e3",37035))
            .toDF("range", "hash", "value1", "value2"))

        partitionInfo = snapshotManagement.updateSnapshot().getPartitionInfoArray.head

        assert(partitionInfo.delta_file_num ==
          (oriDeltaNum % spark.sessionState.conf.getConf(StarLakeSQLConf.PART_MERGE_FILE_MINIMUM_NUM) + 1))

        assert(partitionInfo.read_version == oriReadVersion + 3)
      }


    })
  }

  test("compaction with part merge"){
    withTempDir(dir => {
      withSQLConf(StarLakeSQLConf.PART_MERGE_COMPACTION_COMMIT_ENABLE.key -> "true",
        StarLakeSQLConf.PART_MERGE_FILE_SIZE_FACTOR.key -> "0.0000001",
        StarLakeSQLConf.PART_MERGE_FILE_MINIMUM_NUM.key -> "3"){

        val tablePath = dir.getCanonicalPath

        val data1 = Seq(("range1","hash1","a1",1),("range1","hash2","a2",2),("range1","hash3","a3",3))
          .toDF("range", "hash", "value1", "value2")
        val data2 = Seq(("range1","hash1","b1",11),("range1","hash2","b2",22),("range1","hash3","b3",33))
          .toDF("range", "hash", "value1", "value2")
        val data3 = Seq(("range1","hash1","c1",111),("range1","hash2","c2",222),("range1","hash3","c3",333))
          .toDF("range", "hash", "value1", "value2")
        val data4 = Seq(("range1","hash1","d1",1111),("range1","hash2","d2",2222),("range1","hash3","d3",3333))
          .toDF("range", "hash", "value1", "value2")
        val data5 = Seq(("range1","hash1","e1",11111),("range1","hash2","e2",22222),("range1","hash3","e3",33333))
          .toDF("range", "hash", "value1", "value2")

        data1.write
          .mode("overwrite")
          .format("star")
          .option("rangePartitions", "range")
          .option("hashPartitions", "hash")
          .option("hashBucketNum", "1")
          .save(tablePath)

        val table = StarTable.forPath(tablePath)
        table.upsert(data2)
        table.upsert(data3)
        table.upsert(data4)
        table.upsert(data5)

        val snapshotManagement = SnapshotManagement(tablePath)
        var partitionInfo = snapshotManagement.snapshot.getPartitionInfoArray.head
        val oriDeltaNum = partitionInfo.delta_file_num
        val oriReadVersion = partitionInfo.read_version


        new MergeOpString02().register(spark, "stringOp")
        new MergeOpInt().register(spark, "intOp")

        val mergeOperatorInfo = Map("value2" -> new MergeOpInt())
        table.compaction(mergeOperatorInfo)

        partitionInfo = snapshotManagement.updateSnapshot().getPartitionInfoArray.head

        assert(partitionInfo.delta_file_num == 0)

        assert(partitionInfo.read_version == oriReadVersion + 2)


        checkAnswer(table.toDF.select(
          col("range"),
          col("hash"),
          expr("stringOP(value1) as value1"),
          expr("intOp(value2) as value2")),
          Seq(
            ("range1","hash1","e1",12345),
            ("range1","hash2","e2",24690),
            ("range1","hash3","e3",37035))
            .toDF("range", "hash", "value1", "value2"))

      }


    })
  }






}
