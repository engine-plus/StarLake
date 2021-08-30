package org.apache.spark.sql.star.commands

import com.engineplus.star.meta.MetaVersion
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.star.exception.StarLakeErrors
import org.apache.spark.sql.star.{SnapshotManagement, StarLakeOptions}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

case class CreateMaterialViewCommand(viewName: String,
                                     viewPath: String,
                                     sqlText: String,
                                     rangePartitions: String,
                                     hashPartitions: String,
                                     hashBucketNum: String,
                                     autoUpdate: Boolean) extends RunnableCommand with Command {

  override def run(sparkSession: SparkSession): Seq[Row] = {
    val snapshotManagement = SnapshotManagement(viewPath)
    snapshotManagement.withNewTransaction(tc => {
      //fast failed if view name already exists
      if (MetaVersion.isShortTableNameExists(viewName)._1) {
        throw StarLakeErrors.tableExistsException(viewName)
      }

      val options = Map(
        StarLakeOptions.RANGE_PARTITIONS -> rangePartitions,
        StarLakeOptions.HASH_PARTITIONS -> hashPartitions,
        StarLakeOptions.HASH_BUCKET_NUM -> hashBucketNum,
        StarLakeOptions.SHORT_TABLE_NAME -> viewName,
        StarLakeOptions.CREATE_MATERIAL_VIEW -> "true",
        StarLakeOptions.MATERIAL_SQL_TEXT -> sqlText,
        StarLakeOptions.MATERIAL_AUTO_UPDATE -> autoUpdate.toString
      )

      val data = sparkSession.sql(sqlText)

      val (newFiles, deletedFiles) = WriteIntoTable(
        snapshotManagement,
        SaveMode.ErrorIfExists,
        new StarLakeOptions(options, sparkSession.sessionState.conf),
        configuration = Map.empty, //table.properties,
        data).write(tc, sparkSession)

      tc.commit(newFiles, deletedFiles)


    })

    Nil
  }

}
