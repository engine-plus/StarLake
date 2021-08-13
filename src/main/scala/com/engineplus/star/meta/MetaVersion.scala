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

import com.datastax.driver.core.exceptions.InvalidQueryException
import org.apache.spark.sql.star.exception.{MetaRerunErrors, StarLakeErrors}
import org.apache.spark.sql.star.utils.{PartitionInfo, TableInfo, undoLogInfo}

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import scala.util.matching.Regex


object MetaVersion {
  private val cassandraConnector = MetaUtils.cassandraConnector
  private val database = MetaUtils.DATA_BASE
  private val defaultValue = MetaUtils.UNDO_LOG_DEFAULT_VALUE

  def isTableExists(table_name: String): Boolean = {
    cassandraConnector.withSessionDo(session => {
      try {
        val res = session.execute(
          s"""
             |select table_name from $database.table_info where table_name='$table_name'
      """.stripMargin).one()
        res.getString("table_name")
      } catch {
        case e: InvalidQueryException if e.getMessage
          .contains(s"Keyspace $database does not exist") =>
          MetaTableManage.initDatabaseAndTables()
          return isTableExists(table_name)
        case _: NullPointerException => return false
        case e: Exception => throw e
      }
      true
    })

  }

  def isTableIdExists(table_name: String, table_id: String): Boolean = {
    cassandraConnector.withSessionDo(session => {
      val res = session.execute(
        s"""
           |select table_name from $database.table_info
           |where table_name='$table_name' and table_id='$table_id' allow filtering
      """.stripMargin)
      try {
        res.one().getString("table_name")
      } catch {
        case e: Exception => return false
      }
      true
    })
  }

  //check whether short_table_name exists, and return table path if exists
  def isShortTableNameExists(short_table_name: String): (Boolean, String) = {
    cassandraConnector.withSessionDo(session => {
      val res = session.execute(
        s"""
           |select table_name from $database.table_relation
           |where short_table_name='$short_table_name'
        """.stripMargin)
      val table_name = try {
        res.one().getString("table_name")
      } catch {
        case _: NullPointerException => return (false, "-1")
        case e: Exception => throw e
      }
      (true, table_name)
    })
  }

  //get table path, if not exists, return "not found"
  def getTableNameFromShortTableName(short_table_name: String): String = {
    cassandraConnector.withSessionDo(session => {
      val res = session.execute(
        s"""
           |select table_name from $database.table_relation
           |where short_table_name='$short_table_name'
        """.stripMargin)
      try {
        res.one().getString("table_name")
      } catch {
        case _: NullPointerException => return "not found"
        case e: Exception => throw e
      }
    })
  }

  def isPartitionExists(table_id: String, range_value: String, range_id: String, commit_id: String): Boolean = {
    cassandraConnector.withSessionDo(session => {
      val res = session.execute(
        s"""
           |select range_id from $database.partition_info
           |where table_id='$table_id' and range_value='$range_value'
      """.stripMargin).iterator()
      if (res.hasNext) {
        val exist_range_id = res.next().getString("range_id")
        if (exist_range_id.equals(range_id)) {
          true
        } else {
          throw MetaRerunErrors.partitionChangedException(range_value, commit_id)
        }
      } else {
        false
      }
    })
  }

  def createNewTable(table_name: String,
                     table_id: String,
                     table_schema: String,
                     range_column: String,
                     hash_column: String,
                     setting: String,
                     bucket_num: Int): Unit = {
    cassandraConnector.withSessionDo(session => {
      val table_schema_index = if (table_schema.length > MetaUtils.MAX_SIZE_PER_VALUE) {
        FragmentValue.splitLargeValueIntoFragmentValues(table_id, table_schema)
      } else {
        table_schema
      }

      val res = session.execute(
        s"""
           |insert into $database.table_info
           |(table_name,table_id,table_schema,range_column,hash_column,setting,read_version,pre_write_version,
           |bucket_num,short_table_name)
           |values ('$table_name','$table_id','$table_schema_index','$range_column','$hash_column',$setting,1,1,
           |$bucket_num,'$defaultValue')
           |if not exists
      """.stripMargin)
      if (!res.wasApplied()) {
        throw StarLakeErrors.failedInitTableException(table_name)
      }
    })

  }


  def addPartition(table_id: String, table_name: String, range_id: String, range_value: String): Unit = {
    assert(
      isTableIdExists(table_name, table_id),
      s"Can't find table `$table_name` with id=`$table_id`, it may has been dropped.")

    cassandraConnector.withSessionDo(session => {
      val res = session.execute(
        s"""
           |insert into $database.partition_info
           |(table_id,range_value,range_id,table_name,read_version,pre_write_version,
           |last_update_timestamp,delta_file_num,be_compacted)
           |values ('$table_id','$range_value','$range_id','$table_name',0,0,
           |0,0,true)
           |if not exists
    """.stripMargin)
      if (!res.wasApplied()) {
        throw StarLakeErrors.failedAddPartitionVersionException(table_name, range_value, range_id)
      }
    })
  }


  def getTableInfo(table_name: String): TableInfo = {
    cassandraConnector.withSessionDo(session => {
      val res = session.execute(
        s"""
           |select table_id,table_schema,range_column,hash_column,setting,read_version,bucket_num,short_table_name
           |from $database.table_info where table_name='$table_name'
      """.stripMargin).one()
      val table_id = res.getString("table_id")
      val tmp_table_schema = res.getString("table_schema")

      val parttern = new Regex("\\w{8}(-\\w{4}){3}-\\w{12}")
      val table_schema = if (parttern.findFirstIn(tmp_table_schema).isDefined) {
        FragmentValue.getEntireValue(table_id, tmp_table_schema)
      } else {
        tmp_table_schema
      }
      val short_table_name = res.getString("short_table_name")

      TableInfo(
        table_name,
        table_id,
        table_schema,
        res.getString("range_column"),
        res.getString("hash_column"),
        res.getInt("bucket_num"),
        res.getMap("setting", classOf[String], classOf[String]).toMap,
        res.getInt("read_version"),
        if (short_table_name.equals(defaultValue)) None else Some(short_table_name)
      )
    })

  }

  def getSinglePartitionInfo(table_id: String, range_value: String, range_id: String): PartitionInfo = {
    cassandraConnector.withSessionDo(session => {
      val res = session.execute(
        s"""
           |select table_id,range_id,table_name,range_value,read_version,pre_write_version,
           |last_update_timestamp,delta_file_num,be_compacted
           |from $database.partition_info
           |where table_id='$table_id' and range_value='$range_value' and range_id='$range_id' allow filtering
      """.stripMargin).one()
      PartitionInfo(
        table_id = res.getString("table_id"),
        range_id = res.getString("range_id"),
        table_name = res.getString("table_name"),
        range_value = res.getString("range_value"),
        read_version = res.getLong("read_version"),
        pre_write_version = res.getLong("pre_write_version"),
        last_update_timestamp = res.getLong("last_update_timestamp"),
        delta_file_num = res.getInt("delta_file_num"),
        be_compacted = res.getBool("be_compacted"))
    })

  }

  def getPartitionId(table_id: String, range_value: String): (Boolean, String) = {
    cassandraConnector.withSessionDo(session => {
      val res = session.execute(
        s"""
           |select range_id from $database.partition_info
           |where table_id='$table_id' and range_value='$range_value'
      """.stripMargin)
      try {
        (true, res.one().getString("range_id"))
      } catch {
        case e: Exception => return (false, "")
      }
    })
  }

  def getAllPartitionInfo(table_id: String): Array[PartitionInfo] = {
    cassandraConnector.withSessionDo(session => {
      val partitionVersionBuffer = new ArrayBuffer[PartitionInfo]()
      val res_itr = session.executeAsync(
        s"""
           |select table_id,range_id,table_name,range_value,read_version,pre_write_version,
           |last_update_timestamp,delta_file_num,be_compacted
           |from $database.partition_info
           |where table_id='$table_id'
      """.stripMargin).getUninterruptibly.iterator()
      while (res_itr.hasNext) {
        val res = res_itr.next()
        partitionVersionBuffer += PartitionInfo(
          table_id = res.getString("table_id"),
          range_id = res.getString("range_id"),
          table_name = res.getString("table_name"),
          range_value = res.getString("range_value"),
          read_version = res.getLong("read_version"),
          pre_write_version = res.getLong("pre_write_version"),
          last_update_timestamp = res.getLong("last_update_timestamp"),
          delta_file_num = res.getInt("delta_file_num"),
          be_compacted = res.getBool("be_compacted"))
      }
      partitionVersionBuffer.toArray
    })

  }

  def updatePartitionInfo(info: undoLogInfo): Unit = {
    updatePartitionInfo(
      info.table_id,
      info.range_value,
      info.range_id,
      info.write_version,
      info.delta_file_num,
      info.be_compacted
    )
  }

  def updatePartitionInfo(table_id: String,
                          range_value: String,
                          range_id: String,
                          write_version: Long,
                          delta_file_num: Int,
                          be_compacted: Boolean): Unit = {
    val ori_read_version = write_version - 1
    cassandraConnector.withSessionDo(session => {
      session.execute(
        s"""
           |update $database.partition_info set
           |read_version=$write_version,
           |last_update_timestamp=${System.currentTimeMillis()},
           |delta_file_num=$delta_file_num,
           |be_compacted=$be_compacted
           |where table_id='$table_id' and range_value='$range_value'
           |if range_id='$range_id' and
           |read_version=$ori_read_version
        """.stripMargin)
    })
  }

  def updateTableSchema(table_name: String,
                        table_id: String,
                        table_schema: String,
                        config: Map[String, String],
                        new_read_version: Int): Unit = {
    val setting = MetaUtils.toCassandraSetting(config)
    val ori_read_version = new_read_version - 1
    cassandraConnector.withSessionDo(session => {
      session.execute(
        s"""
           |update $database.table_info set table_schema='$table_schema',setting=$setting,read_version=$new_read_version
           |where table_name='$table_name'
           |if read_version=$ori_read_version and table_id='$table_id'
        """.stripMargin)
    })
  }


  def deleteTableInfo(table_name: String, table_id: String): Unit = {
    cassandraConnector.withSessionDo(session => {
      session.execute(
        s"""
           |delete from $database.table_info
           |where table_name='$table_name'
           |if table_id='$table_id'
      """.stripMargin)
    })
  }

  def deletePartitionInfoByTableId(table_id: String): Unit = {
    cassandraConnector.withSessionDo(session => {
      session.execute(
        s"""
           |delete from $database.partition_info
           |where table_id='$table_id'
      """.stripMargin)
    })
  }

  def deletePartitionInfoByRangeId(table_id: String, range_value: String, range_id: String): Unit = {
    cassandraConnector.withSessionDo(session => {
      session.execute(
        s"""
           |delete from $database.partition_info
           |where table_id='$table_id' and range_value='$range_value'
           |if range_id='$range_id'
      """.stripMargin)
    })
  }

  def deleteShortTableName(short_table_name: String): Unit = {
    cassandraConnector.withSessionDo(session => {
      session.execute(
        s"""
           |delete from $database.table_relation
           |where short_table_name='$short_table_name'
        """.stripMargin)
    })
  }

  def addShortTableName(short_table_name: String,
                        table_name: String): Unit = {
    cassandraConnector.withSessionDo(session => {
      val res = session.execute(
        s"""
           |insert into $database.table_relation
           |(short_table_name,table_name)
           |values ('$short_table_name', '$table_name')
           |if not exists
        """.stripMargin)
      if (!res.wasApplied()) {
        throw StarLakeErrors.failedAddShortTableNameException(short_table_name)
      }
    })
  }

  def updateTableShortName(table_name: String,
                           table_id: String,
                           short_table_name: String): Unit = {
    cassandraConnector.withSessionDo(session => {
      session.execute(
        s"""
           |update $database.table_info set short_table_name='$short_table_name'
           |where table_name='$table_name'
           |if short_table_name='$defaultValue' and table_id='$table_id'
        """.stripMargin)
    })
  }


}

