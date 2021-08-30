package com.engineplus.star.meta

import org.apache.spark.sql.star.utils.{MaterialViewInfo, RelationTable}

object MaterialView {
  private val cassandraConnector = MetaUtils.cassandraConnector
  private val database = MetaUtils.DATA_BASE

  //check whether material view exists or not
  def isMaterialViewExists(view_name: String): Boolean = {
    cassandraConnector.withSessionDo(session => {
      val res = session.execute(
        s"""
           |select table_name from $database.material_view
           |where view_name='$view_name'
        """.stripMargin)
      try {
        res.one().getString("table_name")
      } catch {
        case _: NullPointerException => return false
        case e: Exception => throw e
      }
      true
    })
  }


  def addMaterialView(view_name: String,
                      table_name: String,
                      table_id: String,
                      relation_tables: String,
                      sql_text: String,
                      auto_update: Boolean): Unit = {
    cassandraConnector.withSessionDo(session => {
      val format_sql_text = MetaUtils.formatSqlTextToCassandra(sql_text)
      session.execute(
        s"""
           |insert into $database.material_view
           |(view_name,table_name,table_id,relation_tables,sql_text,auto_update)
           |values ('$view_name', '$table_name', '$table_id', '$relation_tables', '$format_sql_text', $auto_update)
        """.stripMargin)
    })
  }


  def updateMaterialView(view_name: String,
                         relation_tables: String,
                         auto_update: Boolean): Unit = {
    cassandraConnector.withSessionDo(session => {
      session.execute(
        s"""
           |update $database.material_view
           |set relation_tables='$relation_tables',auto_update=$auto_update
           |where view_name='$view_name'
        """.stripMargin)
    })
  }

  def deleteMaterialView(view_name: String): Unit = {
    cassandraConnector.withSessionDo(session => {
      session.execute(
        s"""
           |delete from $database.material_view where view_name='$view_name'
        """.stripMargin)
    })
  }

  def getMaterialViewInfo(view_name: String): Option[MaterialViewInfo] = {
    cassandraConnector.withSessionDo(session => {
      val res = session.execute(
        s"""
           |select sql_text,relation_tables,auto_update from $database.material_view
           |where view_name='$view_name'
        """.stripMargin).one()
      try {
        Some(MaterialViewInfo(
          MetaUtils.formatSqlTextFromCassandra(res.getString("sql_text")),
          res.getString("relation_tables").split(",").map(m => RelationTable.build(m)),
          res.getBool("auto_update")))
      } catch {
        case _: NullPointerException => return None
        case e: Exception => throw e
      }
    })
  }


  def updateMaterialRelationInfo(table_id: String,
                                 table_name: String,
                                 new_views: String): Unit = {
    cassandraConnector.withSessionDo(session => {
      session.execute(
        s"""
           |insert into $database.material_relation
           |(table_id,table_name,material_views)
           |values ('$table_id', '$table_name', '$new_views')
        """.stripMargin)
    })
  }

  def getMaterialRelationInfo(table_id: String): String = {
    cassandraConnector.withSessionDo(session => {
      val res = session.execute(
        s"""
           |select material_views from $database.material_relation where table_id='$table_id'
        """.stripMargin)
      try {
        res.one().getString("material_views")
      } catch {
        case _: NullPointerException => return ""
        case e: Exception => throw e
      }
    })
  }

  def deleteMaterialRelationInfo(table_id: String): Unit = {
    cassandraConnector.withSessionDo(session => {
      session.execute(
        s"""
           |delete from $database.material_relation where table_id='$table_id'
        """.stripMargin)
    })
  }


}
