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

package com.engineplus.star.tables

import java.io.File
import java.net.URI

import com.engineplus.star.livy.{CompactionJob, CompactionJobWithCondition, ExecuteWithLivy}
import com.engineplus.star.tables.execution.StarTableOperations
import org.apache.hadoop.fs.Path
import org.apache.spark.annotation._
import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal}
import org.apache.spark.sql.execution.datasources.v2.merge.parquet.batch.merge_operator.MergeOperator
import org.apache.spark.sql.star.exception.StarLakeErrors
import org.apache.spark.sql.star.sources.StarLakeSourceUtils
import org.apache.spark.sql.star.{SnapshotManagement, StarLakeTableIdentifier, StarLakeUtils}

import scala.collection.JavaConverters._

class StarTable(df: => Dataset[Row], snapshotManagement: SnapshotManagement)
  extends StarTableOperations with Logging {

  /**
    * Apply an alias to the StarTable. This is similar to `Dataset.as(alias)` or
    * SQL `tableName AS alias`.
    *
    */
  @Evolving
  def as(alias: String): StarTable = new StarTable(df.as(alias), snapshotManagement)

  /**
    * Apply an alias to the StarTable. This is similar to `Dataset.as(alias)` or
    * SQL `tableName AS alias`.
    *
    */
  @Evolving
  def alias(alias: String): StarTable = as(alias)


  /**
    * Get a DataFrame (that is, Dataset[Row]) representation of this StarTable.
    *
    */
  @Evolving
  def toDF: Dataset[Row] = df


  /**
    * Delete data from the table that match the given `condition`.
    *
    * @param condition Boolean SQL expression
    */
  @Evolving
  def delete(condition: String): Unit = {
    delete(functions.expr(condition))
  }

  /**
    * Delete data from the table that match the given `condition`.
    *
    * @param condition Boolean SQL expression
    */
  @Evolving
  def delete(condition: Column): Unit = {
    executeDelete(Some(condition.expr))
  }

  /**
    * Delete data from the table.
    *
    */
  @Evolving
  def delete(): Unit = {
    executeDelete(None)
  }


  /**
    * Update rows in the table based on the rules defined by `set`.
    *
    * Scala example to increment the column `data`.
    * {{{
    *    import org.apache.spark.sql.functions._
    *
    *    starTable.update(Map("data" -> col("data") + 1))
    * }}}
    *
    * @param set rules to update a row as a Scala map between target column names and
    *            corresponding update expressions as Column objects.
    */
  @Evolving
  def update(set: Map[String, Column]): Unit = {
    executeUpdate(set, None)
  }

  /**
    * Update rows in the table based on the rules defined by `set`.
    *
    * Java example to increment the column `data`.
    * {{{
    *    import org.apache.spark.sql.Column;
    *    import org.apache.spark.sql.functions;
    *
    *    starTable.update(
    *      new HashMap<String, Column>() {{
    *        put("data", functions.col("data").plus(1));
    *      }}
    *    );
    * }}}
    *
    * @param set rules to update a row as a Java map between target column names and
    *            corresponding update expressions as Column objects.
    */
  @Evolving
  def update(set: java.util.Map[String, Column]): Unit = {
    executeUpdate(set.asScala.toMap, None)
  }

  /**
    * Update data from the table on the rows that match the given `condition`
    * based on the rules defined by `set`.
    *
    * Scala example to increment the column `data`.
    * {{{
    *    import org.apache.spark.sql.functions._
    *
    *    starTable.update(
    *      col("date") > "2018-01-01",
    *      Map("data" -> col("data") + 1))
    * }}}
    *
    * @param condition boolean expression as Column object specifying which rows to update.
    * @param set       rules to update a row as a Scala map between target column names and
    *                  corresponding update expressions as Column objects.
    */
  @Evolving
  def update(condition: Column, set: Map[String, Column]): Unit = {
    executeUpdate(set, Some(condition))
  }

  /**
    * Update data from the table on the rows that match the given `condition`
    * based on the rules defined by `set`.
    *
    * Java example to increment the column `data`.
    * {{{
    *    import org.apache.spark.sql.Column;
    *    import org.apache.spark.sql.functions;
    *
    *    starTable.update(
    *      functions.col("date").gt("2018-01-01"),
    *      new HashMap<String, Column>() {{
    *        put("data", functions.col("data").plus(1));
    *      }}
    *    );
    * }}}
    *
    * @param condition boolean expression as Column object specifying which rows to update.
    * @param set       rules to update a row as a Java map between target column names and
    *                  corresponding update expressions as Column objects.
    */
  @Evolving
  def update(condition: Column, set: java.util.Map[String, Column]): Unit = {
    executeUpdate(set.asScala.toMap, Some(condition))
  }

  /**
    * Update rows in the table based on the rules defined by `set`.
    *
    * Scala example to increment the column `data`.
    * {{{
    *    starTable.updateExpr(Map("data" -> "data + 1")))
    * }}}
    *
    * @param set rules to update a row as a Scala map between target column names and
    *            corresponding update expressions as SQL formatted strings.
    */
  @Evolving
  def updateExpr(set: Map[String, String]): Unit = {
    executeUpdate(toStrColumnMap(set), None)
  }

  /**
    * Update rows in the table based on the rules defined by `set`.
    *
    * Java example to increment the column `data`.
    * {{{
    *    starTable.updateExpr(
    *      new HashMap<String, String>() {{
    *        put("data", "data + 1");
    *      }}
    *    );
    * }}}
    *
    * @param set rules to update a row as a Java map between target column names and
    *            corresponding update expressions as SQL formatted strings.
    */
  @Evolving
  def updateExpr(set: java.util.Map[String, String]): Unit = {
    executeUpdate(toStrColumnMap(set.asScala.toMap), None)
  }

  /**
    * Update data from the table on the rows that match the given `condition`,
    * which performs the rules defined by `set`.
    *
    * Scala example to increment the column `data`.
    * {{{
    *    starTable.update(
    *      "date > '2018-01-01'",
    *      Map("data" -> "data + 1"))
    * }}}
    *
    * @param condition boolean expression as SQL formatted string object specifying
    *                  which rows to update.
    * @param set       rules to update a row as a Scala map between target column names and
    *                  corresponding update expressions as SQL formatted strings.
    */
  @Evolving
  def updateExpr(condition: String, set: Map[String, String]): Unit = {
    executeUpdate(toStrColumnMap(set), Some(functions.expr(condition)))
  }

  /**
    * Update data from the table on the rows that match the given `condition`,
    * which performs the rules defined by `set`.
    *
    * Java example to increment the column `data`.
    * {{{
    *    starTable.update(
    *      "date > '2018-01-01'",
    *      new HashMap<String, String>() {{
    *        put("data", "data + 1");
    *      }}
    *    );
    * }}}
    *
    * @param condition boolean expression as SQL formatted string object specifying
    *                  which rows to update.
    * @param set       rules to update a row as a Java map between target column names and
    *                  corresponding update expressions as SQL formatted strings.
    */
  @Evolving
  def updateExpr(condition: String, set: java.util.Map[String, String]): Unit = {
    executeUpdate(toStrColumnMap(set.asScala.toMap), Some(functions.expr(condition)))
  }


  /**
    * Upsert Star table with source dataframe.
    *
    * Example:
    * {{{
    *   starTable.upsert(sourceDF)
    *   starTable.upsert(sourceDF, "range_col1='a' and range_col2='b'")
    * }}}
    *
    * @param source    source dataframe
    * @param condition you can define a condition to filter Star data
    */
  def upsert(source: DataFrame, condition: String = ""): Unit = {
    condition match {
      case "" => upsert(source, Literal(true))
      case _ => upsert(source, functions.expr(condition).expr)
    }
  }


  def upsert(source: DataFrame): Unit = {
    upsert(source, Literal(true))
  }

  def upsert(source: DataFrame, condition: Expression): Unit = {
    executeUpsert(this, source, condition)
  }


  //by default, force perform compaction on whole table
  def compaction(): Unit = {
    compaction("", true, Map.empty[String, Any])
  }

  def compaction(condition: String): Unit = {
    compaction(condition, true, Map.empty[String, Any])
  }

  def compaction(mergeOperatorInfo: Map[String, Any]): Unit = {
    compaction("", true, mergeOperatorInfo)
  }

  def compaction(condition: String,
                 mergeOperatorInfo: Map[String, Any]): Unit = {
    compaction(condition, true, mergeOperatorInfo)
  }

  def compaction(force: Boolean,
                 mergeOperatorInfo: Map[String, Any] = Map.empty[String, Any]): Unit = {
    compaction("", force, mergeOperatorInfo)
  }

  def compaction(condition: String,
                 force: Boolean): Unit = {
    compaction(condition, true, Map.empty[String, Any])
  }

  /**
    * If `force` set to true, it will ignore delta file num, compaction interval,
    * and base file(first write), compaction will execute if is_compacted is not true.
    *
    */
  def compaction(condition: String,
                 force: Boolean,
                 mergeOperatorInfo: Map[String, Any]): Unit = {
    val newMergeOpInfo = mergeOperatorInfo.map(m => {
      val key =
        if (!m._1.startsWith(StarLakeUtils.MERGE_OP_COL)) {
          s"${StarLakeUtils.MERGE_OP_COL}${m._1}"
        } else {
          m._1
        }
      val value =
        if (m._2.isInstanceOf[MergeOperator[Any]]) {
          m._2.getClass.getName
        } else {
          throw StarLakeErrors.illegalMergeOperatorException(m._2)
        }
      (key, value)
    })

    executeCompaction(df, snapshotManagement, condition, force, newMergeOpInfo)
  }

  /**
    * Execute compaction using livy client.
    *
    * Param conf can be:
    *   - spark.livy.host (default is http://localhost:8998)
    *   - spark.engineplus.star.meta.host
    *   - spark.app.name
    *   - spark.driver.cores
    *   - spark.driver.memory
    *   - spark.driver.memoryOverhead
    *   - spark.executor.cores
    *   - spark.executor.memory
    *   - spark.executor.instances
    *   - spark.executor.memoryOverhead
    *   - spark.livy.upload.jars
    *
    */
  def compactionWithLivy(conf: Map[String, String], condition: String, force: Boolean): Unit = {
    val livyHostKey = "spark.livy.host"
    val livyUploadJarsKey = "spark.livy.upload.jars"

    val confWithLivyHost = if (conf.contains(livyHostKey)) {
      conf
    } else {
      conf ++ Map(livyHostKey -> sparkSession.sessionState.conf.getConfString(livyHostKey))
    }

    val livyClient = ExecuteWithLivy.getLivyClient(confWithLivyHost)
    val externalJars = conf.getOrElse(livyUploadJarsKey, "none")
    if (!externalJars.equals("none")) {
      externalJars.split(",").foreach(path => {
        logInfo(s"add jar $path to livy")
        livyClient.addJar(new URI(path)).get()
      })
    }
//    livyClient.uploadJar(new File(ExecuteWithLivy.getSourcePath(this))).get()
    if (condition.equalsIgnoreCase("")){
      livyClient.submit(new CompactionJob(snapshotManagement.table_name, force))
    }else{
      livyClient.submit(new CompactionJobWithCondition(snapshotManagement.table_name, condition, force))
    }
  }

  def compactionWithLivy(conf: Map[String, String]): Unit = {
    compactionWithLivy(conf, "", true)
  }

  def compactionWithLivy(conf: Map[String, String], force: Boolean): Unit = {
    compactionWithLivy(conf, "", force)
  }

  def compactionWithLivy(conf: Map[String, String], condition: String): Unit = {
    compactionWithLivy(conf, condition, true)
  }

  def cleanup(justList: Boolean = false): Unit = {
    executeCleanup(snapshotManagement, justList)
  }

  def dropTable(): Unit = {
    executeDropTable(snapshotManagement)
  }

  def dropPartition(condition: String): Unit = {
    dropPartition(functions.expr(condition).expr)
  }

  def dropPartition(condition: Expression): Unit = {
    executeDropPartition(snapshotManagement, condition)
  }


}

object StarTable {


  /**
    * Create a StarTable for the data at the given `path`.
    *
    * Note: This uses the active SparkSession in the current thread to read the table data. Hence,
    * this throws error if active SparkSession has not been set, that is,
    * `SparkSession.getActiveSession()` is empty.
    *
    */
  @Evolving
  def forPath(path: String): StarTable = {
    val sparkSession = SparkSession.getActiveSession.getOrElse {
      throw new IllegalArgumentException("Could not find active SparkSession")
    }

    forPath(sparkSession, path)
  }


  /**
    * Create a StarTable for the data at the given `path` using the given SparkSession.
    *
    */
  @Evolving
  def forPath(sparkSession: SparkSession, path: String): StarTable = {
    if (StarLakeUtils.isStarLakeTable(sparkSession, new Path(path))) {
      new StarTable(sparkSession.read.format(StarLakeSourceUtils.SOURCENAME).load(path),
        SnapshotManagement(path))
    } else {
      throw StarLakeErrors.tableNotExistsException(path)
    }
  }

  /**
    * Create a StarTable using the given table or view name using the given SparkSession.
    *
    * Note: This uses the active SparkSession in the current thread to read the table data. Hence,
    * this throws error if active SparkSession has not been set, that is,
    * `SparkSession.getActiveSession()` is empty.
    */
  def forName(tableOrViewName: String): StarTable = {
    val sparkSession = SparkSession.getActiveSession.getOrElse {
      throw new IllegalArgumentException("Could not find active SparkSession")
    }
    forName(sparkSession, tableOrViewName)
  }

  /**
    * Create a StarTable using the given table or view name using the given SparkSession.
    */
  def forName(sparkSession: SparkSession, tableName: String): StarTable = {
    val tableIdent = sparkSession.sessionState.sqlParser.parseTableIdentifier(tableName)
    if (StarLakeUtils.isStarLakeTable(sparkSession, tableIdent)) {
      new StarTable(sparkSession.table(tableName), SnapshotManagement.forTable(sparkSession, tableIdent))
    } else {
      throw StarLakeErrors.notAStarLakeTableException(StarLakeTableIdentifier(table = Some(tableIdent)))
    }
  }


}