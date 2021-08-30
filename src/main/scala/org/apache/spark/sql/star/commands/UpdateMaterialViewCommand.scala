package org.apache.spark.sql.star.commands

import com.engineplus.star.meta.MaterialView
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.PredicateHelper
import org.apache.spark.sql.execution.command.RunnableCommand
import org.apache.spark.sql.star.utils.RelationTable
import org.apache.spark.sql.star.{SnapshotManagement, StarLakeOptions, StarLakeUtils}
import org.apache.spark.sql.{Row, SparkSession}

import scala.collection.mutable.ArrayBuffer

case class UpdateMaterialViewCommand(snapshotManagement: SnapshotManagement) extends RunnableCommand with PredicateHelper with Logging {


  final override def run(sparkSession: SparkSession): Seq[Row] = {
    snapshotManagement.withNewTransaction(tc => {
      val tableInfo = tc.snapshot.getTableInfo
      val materialInfo = MaterialView.getMaterialViewInfo(tableInfo.short_table_name.get)
      assert(materialInfo.isDefined)

      val data = sparkSession.sql(materialInfo.get.sqlText)

      val currentRelationTableVersion = new ArrayBuffer[RelationTable]()
      StarLakeUtils.parseRelationTableInfo(data.queryExecution.executedPlan, currentRelationTableVersion)

      val currentRelationTableVersionMap = currentRelationTableVersion.map(m => (m.tableName, m)).toMap

      val isConsistent = materialInfo.get.relationTables.forall(f => {
        val currentVersion = currentRelationTableVersionMap(f.tableName)
        f.toString.equals(currentVersion.toString)
      })

      if (!isConsistent) {
        //set changed relation table info
        tc.setMaterialInfo(
          materialInfo.get.copy(relationTables = currentRelationTableVersion, isCreatingView = false)
        )
        val options = Map(StarLakeOptions.UPDATE_MATERIAL_VIEW -> "true")
        val newFiles = tc.writeFiles(
          data,
          Some(new StarLakeOptions(options, sparkSession.sessionState.conf)))
        val allFiles = tc.filterFiles(Nil)

        tc.commit(newFiles, allFiles)
      } else {
        logInfo(s"====== Material view `${tableInfo.short_table_name.get}` is latest data, " +
          "it doesn't need update ~")
      }


    })
    Seq.empty
  }

}
