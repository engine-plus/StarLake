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

package org.apache.spark.sql.star.manual_execute_suites

import com.engineplus.star.tables.StarTable
import org.apache.spark.sql.functions.{col, last}
import org.apache.spark.sql.star.SnapshotManagement
import org.apache.spark.sql.star.test.TestUtils
import org.apache.spark.util.Utils

class CompactionDoNotChangeResult {
  def run(): Unit = {
    execute(true)
    execute(false)
  }

  private def execute(onlyOnePartition: Boolean): Unit = {
    val tableName = Utils.createTempDir().getCanonicalPath

    val spark = TestUtils.getSparkSession()

    import spark.implicits._

    try {
      val allData = TestUtils.getData2(5000, onlyOnePartition)
        .toDF("hash", "name", "age", "range")
        .persist()

      allData.select("range", "hash", "name")
        .write
        .option("rangePartitions", "range")
        .option("hashPartitions", "hash")
        .option("hashBucketNum", "2")
        .format("star")
        .save(tableName)

      val sm = SnapshotManagement(tableName)
      var rangeGroup = sm.snapshot.allDataInfo.groupBy(_.range_partitions)
      assert(rangeGroup.forall(_._2.groupBy(_.file_group_id).forall(_._2.length == 1)))

      StarTable.forPath(tableName).upsert(allData.select("range", "hash", "age"))


      rangeGroup = sm.updateSnapshot().allDataInfo.groupBy(_.range_partitions)
      assert(!rangeGroup.forall(_._2.groupBy(_.file_group_id).forall(_._2.length == 1)))


      StarTable.forPath(tableName).compaction(true)
      rangeGroup = sm.updateSnapshot().allDataInfo.groupBy(_.range_partitions)
      assert(rangeGroup.forall(_._2.groupBy(_.file_group_id).forall(_._2.length == 1)))

      val realDF = allData.groupBy("range", "hash")
        .agg(
          last("name").as("n"),
          last("age").as("a"))
        .select(
          col("range"),
          col("hash"),
          col("n").as("name"),
          col("a").as("age"))

      val compactDF = StarTable.forPath(tableName).toDF
        .select("range", "hash", "name", "age")

      TestUtils.checkDFResult(compactDF, realDF)

      StarTable.forPath(tableName).dropTable()

    } catch {
      case e: Exception =>
        StarTable.forPath(tableName).dropTable()
        throw e
    }
  }


}
