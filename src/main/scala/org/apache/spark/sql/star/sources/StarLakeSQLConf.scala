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

package org.apache.spark.sql.star.sources

import org.apache.spark.internal.config.{ConfigBuilder, ConfigEntry}
import org.apache.spark.sql.internal.SQLConf

object StarLakeSQLConf {

  def buildConf(key: String): ConfigBuilder = SQLConf.buildConf(s"spark.engineplus.star.$key")

  def buildStaticConf(key: String): ConfigBuilder =
    SQLConf.buildStaticConf(s"spark.engineplus.star.$key")

  val SCHEMA_AUTO_MIGRATE: ConfigEntry[Boolean] =
    buildConf("schema.autoMerge.enabled")
      .doc("If true, enables schema merging on appends and on overwrites.")
      .booleanConf
      .createWithDefault(false)

  val USE_DELTA_FILE: ConfigEntry[Boolean] =
    buildConf("deltaFile.enabled")
      .doc("If true, enables delta files on specific scene(e.g. upsert).")
      .booleanConf
      .createWithDefault(true)

  val MAX_DELTA_FILE_NUM: ConfigEntry[Int] =
    buildConf("deltaFile.max.num")
      .doc("Maximum delta files allowed, default is 5.")
      .intConf
      .createWithDefault(5)

  val COMPACTION_TIME: ConfigEntry[Long] =
    buildConf("compaction.interval")
      .doc("If the last update time exceeds the set interval, compaction will be triggered, default is 12 hours.")
      .longConf
      .createWithDefault(12 * 60 * 60 * 1000L)

  // History file cleanup interval
  val OLD_VERSION_RETENTION_TIME: ConfigEntry[Long] =
    buildConf("cleanup.interval")
      .doc("Retention time of old version data, default is 5 hours.")
      .longConf
      .createWithDefault(5 * 60 * 60 * 1000L)

  val CLEANUP_PARALLELISM: ConfigEntry[Int] =
    buildConf("cleanup.parallelism")
      .doc("The number of parallelism to list a collection of path recursively when cleanup, default is 50.")
      .intConf
      .createWithDefault(200)

  val CLEANUP_CONCURRENT_DELETE_ENABLE: ConfigEntry[Boolean] =
    buildConf("cleanup.concurrent.delete.enable")
      .doc("If enable delete files concurrently.")
      .booleanConf
      .createWithDefault(true)

  //默认的meta数据库名
  val META_DATABASE_NAME: ConfigEntry[String] =
    buildConf("meta.database.name")
      .doc(
        """
          |Default database of meta tables in Cassandra.
        """.stripMargin)
      .stringConf
      .createWithDefault("star_meta")


  val META_HOST: ConfigEntry[String] =
    buildConf("meta.host")
      .doc(
        """
          |Contact point to connect to the Cassandra cluster.
          |A comma separated list may also be used.("127.0.0.1,192.168.0.1")
        """.stripMargin)
      .stringConf
      .createWithDefault("localhost")

  val META_PORT: ConfigEntry[Int] =
    buildConf("meta.port")
      .doc("Cassandra native connection port.")
      .intConf
      .createWithDefault(9042)

  val META_USERNAME: ConfigEntry[String] =
    buildConf("meta.username")
      .doc(
        """
          |Cassandra username, default is `cassandra`.
        """.stripMargin)
      .stringConf
      .createWithDefault("cassandra")

  val META_PASSWORD: ConfigEntry[String] =
    buildConf("meta.password")
      .doc(
        """
          |Cassandra password, default is `cassandra`.
        """.stripMargin)
      .stringConf
      .createWithDefault("cassandra")

  val META_CONNECT_FACTORY: ConfigEntry[String] =
    buildConf("meta.connect.factory")
      .doc(
        """
          |CassandraConnectionFactory providing connections to the Cassandra cluster.
        """.stripMargin)
      .stringConf
      .createWithDefault("com.engineplus.star.meta.CustomConnectionFactory")

  val META_CONNECT_TIMEOUT: ConfigEntry[Int] =
    buildConf("meta.connect.timeout")
      .doc(
        """
          |Timeout for connecting to cassandra, default is 60s.
        """.stripMargin)
      .intConf
      .createWithDefault(60 * 1000)

  val META_READ_TIMEOUT: ConfigEntry[Int] =
    buildConf("meta.read.timeout")
      .doc(
        """
          |Timeout for reading to cassandra, default is 30s.
        """.stripMargin)
      .intConf
      .createWithDefault(30 * 1000)

  val META_MAX_CONNECT_PER_EXECUTOR: ConfigEntry[Int] =
    buildConf("meta.connections_per_executor_max")
      .doc(
        """
          |Maximum number of connections per Host set on each Executor JVM. Will be
          |updated to DefaultParallelism / Executors for Spark Commands. Defaults to 1
          | if not specifying and not in a Spark Env.
        """.stripMargin)
      .intConf
      .createWithDefault(600)

  val META_GET_LOCK_MAX_ATTEMPTS: ConfigEntry[Int] =
    buildConf("meta.get.lock.max.attempts")
      .doc(
        """
          |The maximum times for a commit attempts to acquire the lock.
        """.stripMargin)
      .intConf
      .createWithDefault(5)

  val META_GET_LOCK_WAIT_INTERVAL: ConfigEntry[Int] =
    buildConf("meta.acquire.write.lock.wait.interval")
      .doc(
        """
          |The wait time when a commit failed to get write lock because another job is committing.
        """.stripMargin)
      .intConf
      .createWithDefault(5)

  val META_GET_LOCK_RETRY_INTERVAL: ConfigEntry[Int] =
    buildConf("meta.acquire.write.lock.retry.interval")
      .doc(
        """
          |The interval time when a commit failed to get write lock.
          |The commit will wait a random time between 0 and RETRY_INTERVAL seconds.
        """.stripMargin)
      .intConf
      .createWithDefault(20)

  val META_COMMIT_TIMEOUT: ConfigEntry[Long] =
    buildConf("meta.commit.timeout")
      .doc(
        """
          |The maximum timeout for a committer.
        """.stripMargin)
      .longConf
      .createWithDefault(20 * 1000L)

  val META_UNDO_LOG_TIMEOUT: ConfigEntry[Long] =
    buildConf("meta.undo_log.timeout")
      .doc(
        """
          |The maximum timeout for undo log(exclude Commit type).
          |This parameter will only be used in Cleanup operation.
        """.stripMargin)
      .longConf
      .createWithDefault(30 * 60 * 1000L)

  val META_STREAMING_INFO_TIMEOUT: ConfigEntry[Long] =
    buildConf("meta.streaming_info.timeout")
      .doc(
        """
          |The maximum timeout for streaming info.
          |This parameter will only be used in Cleanup operation.
        """.stripMargin)
      .longConf
      .createWithDefault(12 * 60 * 60 * 1000L)

  val META_MAX_COMMIT_ATTEMPTS: ConfigEntry[Int] =
    buildConf("meta.commit.max.attempts")
      .doc(
        """
          |The maximum times for a job attempts to commit.
        """.stripMargin)
      .intConf
      .createWithDefault(5)

  val META_MAX_SIZE_PER_VALUE: ConfigEntry[Int] =
    buildConf("meta.max.size.per.value")
      .doc(
        """
          |The maximum size for undo log value(e.g. table_info.table_schema).
          |If value size exceed this limit, it will be split into some fragment values.
        """.stripMargin)
      .intConf
      .createWithDefault(50 * 1024)

  //dorp table await time
  val DROP_TABLE_WAIT_SECONDS: ConfigEntry[Int] =
    buildConf("drop.table.wait.seconds")
      .doc(
        """
          |When dropping table or partition, we need wait a few seconds for the other commits to be completed.
        """.stripMargin)
      .intConf
      .createWithDefault(1)

  val ALLOW_FULL_TABLE_UPSERT: ConfigEntry[Boolean] =
    buildConf("full.partitioned.table.scan.enabled")
      .doc("If true, enables full table scan when upsert.")
      .booleanConf
      .createWithDefault(false)

  val PARQUET_BLOCK_SIZE: ConfigEntry[Long] =
    buildConf("parquet.block.size")
      .doc("Parquet block size.")
      .longConf
      .createWithDefault(32 * 1024 * 1024L)


  val PARQUET_COMPRESSION: ConfigEntry[String] =
    buildConf("parquet.compression")
      .doc(
        """
          |Parquet compression type.
        """.stripMargin)
      .stringConf
      .createWithDefault("snappy")

  val PARQUET_COMPRESSION_ENABLE: ConfigEntry[Boolean] =
    buildConf("parquet.compression.enable")
      .doc(
        """
          |Whether to use parquet compression.
        """.stripMargin)
      .booleanConf
      .createWithDefault(true)

  val BUCKET_SCAN_MULTI_PARTITION_ENABLE: ConfigEntry[Boolean] =
    buildConf("bucket.scan.multi.partition.enable")
      .doc(
        """
          |Hash partitioned table can read multi-partition data partitioned by hash keys without shuffle,
          |this parameter controls whether this feature is enabled or not.
          |Using this feature, the parallelism will equal to hash bucket num.
        """.stripMargin)
      .booleanConf
      .createWithDefault(false)

  val PART_MERGE_ENABLE: ConfigEntry[Boolean] =
    buildConf("part.merge.enable")
      .doc(
        """
          |If true, part files merging will be used to avoid OOM when it has too many delta files.
        """.stripMargin)
      .booleanConf
      .createWithDefault(false)

  val PART_MERGE_COMPACTION_COMMIT_ENABLE: ConfigEntry[Boolean] =
    buildConf("part.merge.compaction.commit.enable")
      .doc(
        """
          |If true, it will commit the compacted files into meta store, and the later reader can read faster.
          |Note that if you read a column by self-defined merge operator, the compacted result should also use
          |this merge operator, make sure that the result is expected or disable compaction commit.
        """.stripMargin)
      .booleanConf
      .createWithDefault(true)

  val PART_MERGE_FILE_MINIMUM_NUM: ConfigEntry[Int] =
    buildConf("part.merge.file.minimum.num")
      .doc(
        """
          |If delta file num more than this count, we will check for part merge.
        """.stripMargin)
      .intConf
      .createWithDefault(5)


  val PART_MERGE_FILE_SIZE_FACTOR: ConfigEntry[Double] =
    buildConf("part.merge.file.size.factor")
      .doc(
        """
          |File size factor to calculate part merge max size.
          |Expression: PART_MERGE_FILE_MINIMUM_NUM * PART_MERGE_FILE_SIZE_FACTOR * 128M
        """.stripMargin)
      .doubleConf
      .createWithDefault(0.1)

  val ASYNC_READER_ENABLE: ConfigEntry[Boolean] =
    buildConf("async.reader.enable")
      .doc(
        """
          |Whether async reader can be used.
        """.stripMargin)
      .booleanConf
      .createWithDefault(true)

  val AUTO_UPDATE_MATERIAL_VIEW_ENABLE: ConfigEntry[Boolean] =
    buildConf("auto.update.materialView.enable")
      .doc(
        """
          |Whether update material views when data changed.
          |If true, it will check all material views associate with
        """.stripMargin)
      .booleanConf
      .createWithDefault(false)

  val ALLOW_STALE_MATERIAL_VIEW: ConfigEntry[Boolean] =
    buildConf("allow.stale.materialView")
      .doc(
        """
          |If true, material view with stale data will be read.
        """.stripMargin)
      .booleanConf
      .createWithDefault(false)

}
