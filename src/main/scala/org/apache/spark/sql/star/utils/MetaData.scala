package org.apache.spark.sql.star.utils

import com.engineplus.star.meta.{CommitState, CommitType, MetaUtils}
import com.fasterxml.jackson.annotation.JsonIgnore
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.execution.datasources.BucketingUtils
import org.apache.spark.sql.types.{DataType, StructType}

case class MetaInfo(table_info: TableInfo,
                    partitionInfoArray: Array[PartitionInfo],
                    commit_type: CommitType,
                    commit_id: String = "",
                    query_id: String = "",
                    batch_id: Long = -1L)

case class PartitionInfo(table_id: String,
                         range_id: String,
                         table_name: String,
                         range_value: String,
                         read_version: Long,
                         pre_write_version: Long,
                         read_files: Array[DataFileInfo] = Array.empty[DataFileInfo],
                         add_files: Array[DataFileInfo] = Array.empty[DataFileInfo],
                         expire_files: Array[DataFileInfo] = Array.empty[DataFileInfo],
                         last_update_timestamp: Long = -1L,
                         delta_file_num: Int = 0,
                         be_compacted: Boolean = false) {
  override def toString: String = {
    s"partition info: {\ntable_name: $table_name,\nrange_value: $range_value,\nread_version: $read_version," +
      s"\ndelta_file_num: $delta_file_num,\nbe_compacted: $be_compacted\n}"
  }
}

// table_schema是json数据
// range_column和hash_column都是字符串，不是json
//hash_partition_column 可以包含多个分区key，用逗号分隔
case class TableInfo(table_name: String,
                     table_id: String,
                     table_schema: String = null,
                     range_column: String = "",
                     hash_column: String = "",
                     bucket_num: Int = -1,
                     configuration: Map[String, String] = Map.empty,
                     schema_version: Int = 1) {

  lazy val table_path: Path = new Path(table_name)
  lazy val range_partition_columns: Seq[String] = range_partition_schema.fieldNames
  lazy val hash_partition_columns: Seq[String] = hash_partition_schema.fieldNames

  /** Returns the schema as a [[StructType]] */
  //包含分区字段的全表schema
  @JsonIgnore
  lazy val schema: StructType =
  Option(table_schema).map { s =>
    DataType.fromJson(s).asInstanceOf[StructType]
  }.getOrElse(StructType.apply(Nil))

  //range分区字段
  @JsonIgnore
  lazy val range_partition_schema: StructType =
  if (range_column.equalsIgnoreCase("")) {
    StructType.apply(Nil)
  } else {
    StructType(range_column.split(",").map(c => schema(c)))
  }

  //hash分区字段
  @JsonIgnore
  lazy val hash_partition_schema: StructType =
  if (hash_column.equalsIgnoreCase("")) {
    StructType.apply(Nil)
  } else {
    StructType(hash_column.split(",").map(c => schema(c)))
  }

  //所有分区字段
  lazy val partition_schema: StructType = range_partition_schema.merge(hash_partition_schema)

  //hash字段属于data_schema !!!
  private lazy val range_partition_set: Set[String] = range_column.split(",").toSet
  //除range分区字段外的其他数据字段
  @JsonIgnore
  lazy val data_schema: StructType = StructType(schema.filterNot(f => range_partition_set.contains(f.name)))

  lazy val partition_cols: Seq[String] = {
    var seq = Seq.empty[String]
    if (range_column.nonEmpty) {
      seq = seq ++ range_column.split(",")
    }
    if (hash_column.nonEmpty) {
      seq = seq ++ hash_column.split(",")
    }
    seq
  }

  lazy val format: Format = Format()

}


//单个文件的信息
case class DataFileInfo(file_path: String,
                        range_partitions: Map[String, String],
                        size: Long,
                        modification_time: Long,
                        write_version: Long,
                        is_base_file: Boolean,
                        file_exist_cols: String = "") {
  lazy val range_key: String = MetaUtils.getPartitionKeyFromMap(range_partitions)

  //identify for merge read
  lazy val range_version: String = range_key + "-" + write_version.toString

  lazy val file_group_id: Int = BucketingUtils
    .getBucketId(new Path(file_path).getName)
    .getOrElse(sys.error(s"Invalid bucket file $file_path"))

  //转化为待删除文件
  def expire(deleteTime: Long): DataFileInfo = this.copy(modification_time = deleteTime)
}


case class PartitionFilterInfo(range_id: String,
                               range_value: String,
                               range_partitions: Map[String, String])


/**
  * commit状态信息，用于判断commit的状态，并根据状态执行相应的操作
  *
  * @param state     commit状态
  * @param commit_id commit id
  * @param tag       回滚标识符
  * @param timestamp 时间戳
  */
case class commitStateInfo(state: CommitState.Value,
                           table_name: String,
                           table_id: String,
                           commit_id: String,
                           tag: Int,
                           timestamp: Long)

/**
  * undo log 信息
  *
  * @param commit_type   标识类型，有partition和data
  * @param table_name    表名
  * @param range_value   分区名
  * @param commit_id     commit id
  * @param file_path     文件路径
  *                      //  * @param tag            commit类型的回滚标识，0表示commit，大于0表示回滚，其他类型为 -1
  * @param write_version 写版本号
  */
case class undoLogInfo(commit_type: String,
                       table_id: String,
                       commit_id: String,
                       range_id: String,
                       file_path: String,
                       table_name: String,
                       range_value: String,
                       tag: Int,
                       write_version: Long,
                       timestamp: Long,
                       size: Long,
                       modification_time: Long,
                       table_schema: String,
                       setting: Map[String, String],
                       file_exist_cols: String,
                       delta_file_num: Int,
                       be_compacted: Boolean,
                       is_base_file: Boolean,
                       query_id: String,
                       batch_id: Long)

case class Format(provider: String = "parquet",
                  options: Map[String, String] = Map.empty)
