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

import java.net.URI

import com.engineplus.star.livy.{CompactionJob, CompactionJobWithCondition, ExecuteWithLivy}
import com.engineplus.star.meta.MetaVersion
import com.engineplus.star.tables.execution.StarTableOperations
import org.apache.hadoop.fs.Path
import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.execution.datasources.v2.merge.parquet.batch.merge_operator.MergeOperator
import org.apache.spark.sql.star.commands.CreateMaterialViewCommand
import org.apache.spark.sql.star.exception.StarLakeErrors
import org.apache.spark.sql.star.sources.StarLakeSourceUtils
import org.apache.spark.sql.star.{SnapshotManagement, StarLakeUtils}

import scala.collection.JavaConverters._

class StarTable(df: => Dataset[Row], snapshotManagement: SnapshotManagement)
  extends StarTableOperations with Logging {

  /**
    * Apply an alias to the StarTable. This is similar to `Dataset.as(alias)` or
    * SQL `tableName AS alias`.
    *
    */
  def as(alias: String): StarTable = new StarTable(df.as(alias), snapshotManagement)

  /**
    * Apply an alias to the StarTable. This is similar to `Dataset.as(alias)` or
    * SQL `tableName AS alias`.
    *
    */
  def alias(alias: String): StarTable = as(alias)


  /**
    * Get a DataFrame (that is, Dataset[Row]) representation of this StarTable.
    *
    */
  def toDF: Dataset[Row] = df


  /**
    * Delete data from the table that match the given `condition`.
    *
    * @param condition Boolean SQL expression
    */
  def delete(condition: String): Unit = {
    delete(functions.expr(condition))
  }

  /**
    * Delete data from the table that match the given `condition`.
    *
    * @param condition Boolean SQL expression
    */
  def delete(condition: Column): Unit = {
    executeDelete(Some(condition.expr))
  }

  /**
    * Delete data from the table.
    *
    */
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

  def compaction(condition: String,
                 force: Boolean,
                 mergeOperatorInfo: java.util.Map[String, Any]): Unit = {
    compaction(condition, force, mergeOperatorInfo.asScala.toMap)
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
      val value = m._2 match {
        case cls: MergeOperator[Any] => cls.getClass.getName
        case name: String => name
        case _ => throw StarLakeErrors.illegalMergeOperatorException(m._2)
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
    if (condition.equalsIgnoreCase("")) {
      livyClient.submit(new CompactionJob(snapshotManagement.table_name, force))
    } else {
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

  def dropTable(): Boolean = {
    executeDropTable(snapshotManagement)
    true
  }

  def dropPartition(condition: String): Unit = {
    dropPartition(functions.expr(condition).expr)
  }

  def dropPartition(condition: Expression): Unit = {
    assert(snapshotManagement.snapshot.getTableInfo.range_partition_columns.nonEmpty,
      s"Table `${snapshotManagement.table_name}` is not a range partitioned table, dropTable command can't use on it.")
    executeDropPartition(snapshotManagement, condition)
  }

  def updateMaterialView(): Unit = {
    val tableInfo = snapshotManagement.snapshot.getTableInfo
    if (!tableInfo.is_material_view) {
      throw StarLakeErrors.notMaterialViewException(tableInfo.table_name, tableInfo.short_table_name.getOrElse("None"))
    }

    executeUpdateForMaterialView(snapshotManagement)
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
  def forPath(sparkSession: SparkSession, path: String): StarTable = {
    if (StarLakeUtils.isStarLakeTable(sparkSession, new Path(path))) {
      new StarTable(sparkSession.read.format(StarLakeSourceUtils.SOURCENAME).load(path),
        SnapshotManagement(path))
    } else {
      throw StarLakeErrors.tableNotExistsException(path)
    }
  }

  /**
    * Create a StarTable using the given table name using the given SparkSession.
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
    val (exists, tablePath) = MetaVersion.isShortTableNameExists(tableName)
    if (exists) {
      val starName = if (tableName.startsWith("star.")) tableName else s"star.$tableName"
      new StarTable(sparkSession.table(starName),
        SnapshotManagement(tablePath))
    } else {
      throw StarLakeErrors.notAStarLakeTableException(tableName)
    }
  }

  def isStarTable(tablePath: String): Boolean = {
    StarLakeUtils.isStarLakeTable(tablePath)
  }

  def registerMergeOperator(spark: SparkSession, className: String, funName: String): Unit = {
    StarLakeUtils.getClass(className).getConstructors()(0)
      .newInstance()
      .asInstanceOf[MergeOperator[Any]]
      .register(spark, funName)
  }

  def createMaterialView(viewName: String,
                         viewPath: String,
                         sqlText: String,
                         rangePartitions: String = "",
                         hashPartitions: String = "",
                         hashBucketNum: Int = -1,
                         autoUpdate: Boolean = false): Unit = {
    CreateMaterialViewCommand(
      viewName,
      viewPath,
      sqlText,
      rangePartitions,
      hashPartitions,
      hashBucketNum.toString,
      autoUpdate)
      .run(SparkSession.active)
  }

  class TableCreator {
    private[this] val options = new scala.collection.mutable.HashMap[String, String]
    private[this] var writeData: Dataset[_] = _
    private[this] var tablePath: String = _

    def data(data: Dataset[_]): TableCreator = {
      writeData = data
      this
    }

    def path(path: String): TableCreator = {
      tablePath = path
      this
    }

    //set range partition columns, join with a comma
    def rangePartitions(rangePartitions: String): TableCreator = {
      options.put("rangePartitions", rangePartitions)
      this
    }

    def rangePartitions(rangePartitions: Seq[String]): TableCreator = {
      options.put("rangePartitions", rangePartitions.mkString(","))
      this
    }

    //set hash partition columns, join with a comma
    def hashPartitions(hashPartitions: String): TableCreator = {
      options.put("hashPartitions", hashPartitions)
      this
    }

    def hashPartitions(hashPartitions: Seq[String]): TableCreator = {
      options.put("hashPartitions", hashPartitions.mkString(","))
      this
    }

    def hashBucketNum(hashBucketNum: Int): TableCreator = {
      options.put("hashBucketNum", hashBucketNum.toString)
      this
    }

    def hashBucketNum(hashBucketNum: String): TableCreator = {
      options.put("hashBucketNum", hashBucketNum)
      this
    }

    //set a short table name
    def shortTableName(shortTableName: String): TableCreator = {
      options.put("shortTableName", shortTableName)
      this
    }

    def create(): Unit = {
      val writer = writeData.write.format(StarLakeSourceUtils.NAME).mode("overwrite")
      options.foreach(f => writer.option(f._1, f._2))
      writer.save(tablePath)
    }


  }

  def createTable(data: Dataset[_], tablePath: String): TableCreator =
    new TableCreator().data(data).path(tablePath)


}
