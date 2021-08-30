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

package org.apache.spark.sql.star

import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.star.StarLakeOptions._
import org.apache.spark.sql.star.exception.StarLakeErrors
import org.apache.spark.sql.star.sources.{StarLakeDataSource, StarLakeSQLConf}

import scala.util.Try


trait StarLakeOptionParser {
  protected def sqlConf: SQLConf

  protected def options: CaseInsensitiveMap[String]

  def toBoolean(input: String, name: String): Boolean = {
    Try(input.toBoolean).toOption.getOrElse {
      throw StarLakeErrors.illegalStarLakeOptionException(name, input, "must be 'true' or 'false'")
    }
  }
}


trait StarLakeWriteOptions
  extends StarLakeWriteOptionsImpl
    with StarLakeOptionParser {

  import StarLakeOptions._

  val replaceWhere: Option[String] = options.get(REPLACE_WHERE_OPTION)
}

trait StarLakeWriteOptionsImpl extends StarLakeOptionParser {
  /**
    * Whether the user has enabled auto schema merging in writes using either a DataFrame option
    * or SQL Session configuration. Automerging is off when table ACLs are enabled.
    * We always respect the DataFrame writer configuration over the session config.
    */
  def canMergeSchema: Boolean = {
    options.get(MERGE_SCHEMA_OPTION)
      .map(toBoolean(_, MERGE_SCHEMA_OPTION))
      .getOrElse(sqlConf.getConf(StarLakeSQLConf.SCHEMA_AUTO_MIGRATE))
  }

  /**
    * Whether to allow overwriting the schema of a Star table in an overwrite mode operation. If
    * ACLs are enabled, we can't change the schema of an operation through a write, which requires
    * MODIFY permissions, when schema changes require OWN permissions.
    */
  def canOverwriteSchema: Boolean = {
    options.get(OVERWRITE_SCHEMA_OPTION).exists(toBoolean(_, OVERWRITE_SCHEMA_OPTION))
  }

  //Compatible with df.write.partitionBy , option with "rangePartitions" has higher priority
  def rangePartitions: String = {
    options.get(RANGE_PARTITIONS)
      .getOrElse(
        options.get(PARTITION_BY)
          .map(StarLakeDataSource.decodePartitioningColumns)
          .getOrElse(Nil).mkString(","))
  }

  def hashPartitions: String = {
    options.get(HASH_PARTITIONS).getOrElse("")
  }

  def hashBucketNum: Int = {
    options.get(HASH_BUCKET_NUM).getOrElse("-1").toInt
  }

  def allowDeltaFile: Boolean = {
    options.get(AllowDeltaFile)
      .map(toBoolean(_, AllowDeltaFile))
      .getOrElse(sqlConf.getConf(StarLakeSQLConf.USE_DELTA_FILE))
  }

  def shortTableName: Option[String] = {
    val shortTableName = options.get(SHORT_TABLE_NAME).getOrElse("")
    if (shortTableName.isEmpty) {
      None
    } else {
      Some(shortTableName)
    }
  }

  def createMaterialView: Boolean = {
    options.get(CREATE_MATERIAL_VIEW).exists(toBoolean(_, CREATE_MATERIAL_VIEW))
  }

  def updateMaterialView: Boolean = {
    options.get(UPDATE_MATERIAL_VIEW).exists(toBoolean(_, UPDATE_MATERIAL_VIEW))
  }

  def materialSQLText: String = {
    options.get(MATERIAL_SQL_TEXT).getOrElse("")
  }

  def materialAutoUpdate: Boolean = {
    options.get(MATERIAL_AUTO_UPDATE)
      .exists(toBoolean(_, MATERIAL_AUTO_UPDATE))
  }


}

/**
  * Options for the star lake source.
  */
class StarLakeOptions(@transient protected[star] val options: CaseInsensitiveMap[String],
                      @transient protected val sqlConf: SQLConf)
  extends StarLakeWriteOptions with StarLakeOptionParser with Serializable {

  def this(options: Map[String, String], conf: SQLConf) = this(CaseInsensitiveMap(options), conf)
}


object StarLakeOptions {

  /** An option to overwrite only the data that matches predicates over partition columns. */
  val REPLACE_WHERE_OPTION = "replaceWhere"
  /** An option to allow automatic schema merging during a write operation. */
  val MERGE_SCHEMA_OPTION = "mergeSchema"
  /** An option to allow overwriting schema and partitioning during an overwrite write operation. */
  val OVERWRITE_SCHEMA_OPTION = "overwriteSchema"

  val PARTITION_BY = "__partition_columns"
  val RANGE_PARTITIONS = "rangePartitions"
  val HASH_PARTITIONS = "hashPartitions"
  val HASH_BUCKET_NUM = "hashBucketNum"

  val SHORT_TABLE_NAME = "shortTableName"

  val CREATE_MATERIAL_VIEW = "createStarLakeMaterialView"
  val UPDATE_MATERIAL_VIEW = "updateStarLakeMaterialView"
  val MATERIAL_SQL_TEXT = "materialSQLText"
  val MATERIAL_AUTO_UPDATE = "materialAutoUpdate"

  /** whether it is allowed to use delta file */
  val AllowDeltaFile = "allowDeltaFile"

}
