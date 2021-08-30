package org.apache.spark.sql.star.commands

import com.engineplus.star.meta.{MaterialView, MetaUtils, MetaVersion}
import com.engineplus.star.tables.StarTable
import org.apache.spark.sql.functions._
import org.apache.spark.sql.star.SnapshotManagement
import org.apache.spark.sql.star.sources.StarLakeSQLConf
import org.apache.spark.sql.star.test.StarLakeSQLCommandTest
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.{AnalysisException, QueryTest}
import org.apache.spark.util.Utils
import org.scalatest.BeforeAndAfter

class MaterialViewSuite extends QueryTest
  with SharedSparkSession with StarLakeSQLCommandTest with BeforeAndAfter {

  import testImplicits._


  val tablePath1: String = Utils.createTempDir().getCanonicalPath
  val tablePath2: String = Utils.createTempDir().getCanonicalPath
  val tableName1 = "t1"
  val tableName2 = "t2"


  before {
    Seq((1, "a", "1"), (2, "b", "1"), (3, "c", "1"), (4, "d", "2")).toDF("k", "v", "p")
      .write.mode("overwrite")
      .format("star")
      .option("rangePartitions", "p")
      .option("hashPartitions", "k")
      .option("hashBucketNum", "2")
      .option("shortTableName", tableName1)
      .save(tablePath1)

    Seq((1, "aa", 11), (2, "bb", 11), (3, "cc", 11), (4, "dd", 22)).toDF("key", "value", "partition")
      .write.mode("overwrite")
      .format("star")
      .option("rangePartitions", "partition")
      .option("hashPartitions", "key")
      .option("hashBucketNum", "2")
      .option("shortTableName", tableName2)
      .save(tablePath2)
  }

  after {
    StarTable.forName(tableName1).dropTable()
    StarTable.forName(tableName2).dropTable()
  }


  test("create material view - with partition:") {
    val withPart = false
    withTempDir(f => {
      val viewPath = f.getCanonicalPath
      val viewName = "mv1"
      val sqlText = s"select * from star.$tableName1 a join star.$tableName2 b on a.k=b.key where a.k<3"

      if (withPart) {
        StarTable.createMaterialView(
          viewName,
          viewPath,
          sqlText,
          "p",
          "k",
          2,
          true)
      } else {
        StarTable.createMaterialView(
          viewName = viewName,
          viewPath = viewPath,
          sqlText = sqlText,
          autoUpdate = true)
      }

      checkAnswer(
        StarTable.forName(viewName).toDF.select("k", "v", "p", "key", "value", "partition"),
        Seq((1, "a", "1", 1, "aa", 11), (2, "b", "1", 2, "bb", 11)).toDF("k", "v", "p", "key", "value", "partition")
      )

      assert(SnapshotManagement(viewPath).getTableInfoOnly.is_material_view)
      //check material_view info
      val materialViewInfo = MaterialView.getMaterialViewInfo(viewName)
      assert(materialViewInfo.get.sqlText == sqlText)
      assert(materialViewInfo.get.autoUpdate)
      val relationTables = materialViewInfo.get.relationTables
      assert(relationTables.toSet.size == 2)
      relationTables.foreach(t => {
        val snapshot = SnapshotManagement(t.tableName).snapshot
        assert(snapshot.getTableInfo.table_id == t.tableId)
        assert(t.partitionInfo.length == 2)
        t.partitionInfo.foreach(f => {
          val partition = snapshot.getPartitionInfoArray.filter(p => p.range_id.equals(f._1))
          assert(partition.length == 1)
          partition.head.read_version.toString.equals(f._2)
        })

        //check material_relation info
        val relationViews = MaterialView.getMaterialRelationInfo(t.tableId)
        assert(relationViews.equals(viewName))
      })
    })

  }


  Seq(true, false).foreach(withPart => {
    test(s"create material view - with partition: $withPart") {
      withTempDir(f => {
        val viewPath = f.getCanonicalPath
        val viewName = "mv1"
        val sqlText = s"select * from star.$tableName1 a join star.$tableName2 b on a.k=b.key where a.k<3"

        if (withPart) {
          StarTable.createMaterialView(
            viewName,
            viewPath,
            sqlText,
            "p",
            "k",
            2,
            true)
        } else {
          StarTable.createMaterialView(
            viewName = viewName,
            viewPath = viewPath,
            sqlText = sqlText,
            autoUpdate = true)
        }

        checkAnswer(
          StarTable.forName(viewName).toDF.select("k", "v", "p", "key", "value", "partition"),
          Seq((1, "a", "1", 1, "aa", 11), (2, "b", "1", 2, "bb", 11)).toDF("k", "v", "p", "key", "value", "partition")
        )

        assert(SnapshotManagement(viewPath).getTableInfoOnly.is_material_view)
        //check material_view info
        val materialViewInfo = MaterialView.getMaterialViewInfo(viewName)
        assert(materialViewInfo.get.sqlText == sqlText)
        assert(materialViewInfo.get.autoUpdate)
        val relationTables = materialViewInfo.get.relationTables
        assert(relationTables.toSet.size == 2)
        relationTables.foreach(t => {
          val snapshot = SnapshotManagement(t.tableName).snapshot
          assert(snapshot.getTableInfo.table_id == t.tableId)
          assert(t.partitionInfo.length == 2)
          t.partitionInfo.foreach(f => {
            val partition = snapshot.getPartitionInfoArray.filter(p => p.range_id.equals(f._1))
            assert(partition.length == 1)
            partition.head.read_version.toString.equals(f._2)
          })

          //check material_relation info
          val relationViews = MaterialView.getMaterialRelationInfo(t.tableId)
          assert(relationViews.equals(viewName))
        })
      })

    }

  })


  test("create two material view and drop one") {
    withTempDir(dir => {
      withTempDir(f => {
        val viewPath1 = f.getCanonicalPath
        val viewName1 = "mv1"
        val sqlText1 = s"select * from star.$tableName1 a join star.$tableName2 b on a.k=b.key where a.k>=3"

        StarTable.createMaterialView(viewName1, viewPath1, sqlText1, "p")
        checkAnswer(
          StarTable.forName(viewName1).toDF.select("k", "v", "p", "key", "value", "partition"),
          Seq((3, "c", "1", 3, "cc", 11), (4, "d", "2", 4, "dd", 22)).toDF("k", "v", "p", "key", "value", "partition")
        )

        //second view
        val viewPath2 = dir.getCanonicalPath
        val viewName2 = "mv2"
        val sqlText2 = s"select k,v from star.$tableName1 where v='c' and p='1'"

        StarTable.createMaterialView(viewName2, viewPath2, sqlText2, "", "k", 2)
        checkAnswer(StarTable.forName(viewName2).toDF.select("k", "v"), Seq((3, "c")).toDF("k", "v"))

        assert(SnapshotManagement(viewPath1).getTableInfoOnly.is_material_view)
        assert(SnapshotManagement(viewPath2).getTableInfoOnly.is_material_view)
        //check material_view info
        val materialViewInfo = MaterialView.getMaterialViewInfo(viewName2)
        assert(!materialViewInfo.get.autoUpdate)
        assert(materialViewInfo.get.sqlText == sqlText2)
        val relationTables = materialViewInfo.get.relationTables
        assert(relationTables.toSet.size == 1)
        relationTables.foreach(t => {
          val snapshot = SnapshotManagement(t.tableName).snapshot
          assert(snapshot.getTableInfo.table_id == t.tableId)
          assert(t.partitionInfo.length == 1)
          t.partitionInfo.foreach(f => {
            val partition = snapshot.getPartitionInfoArray.filter(p => p.range_id.equals(f._1))
            assert(partition.length == 1)
            partition.head.read_version.toString.equals(f._2)
          })

          //check material_relation info
          val relationViews = MaterialView.getMaterialRelationInfo(t.tableId).split(",")
          assert(relationViews.length == 2)
          assert(relationViews.contains(viewName1) && relationViews.contains(viewName2))
        })


        //drop one view to check info
        StarTable.forName(viewName1).dropTable()

        val materialViewAfterDrop1 = MaterialView.getMaterialViewInfo(viewName1)
        assert(materialViewAfterDrop1.isEmpty)

        val materialViewAfterDrop2 = MaterialView.getMaterialViewInfo(viewName2)
        assert(materialViewAfterDrop2.get.sqlText == sqlText2)
        assert(!materialViewAfterDrop2.get.autoUpdate)

        val tableInfo_t1 = SnapshotManagement(tablePath1).getTableInfoOnly
        val tableInfo_t2 = SnapshotManagement(tablePath2).getTableInfoOnly

        assert(MaterialView.getMaterialRelationInfo(tableInfo_t1.table_id).equals(viewName2))
        assert(MaterialView.getMaterialRelationInfo(tableInfo_t2.table_id).equals(""))
      })
    })
  }

  Seq(true, false).foreach(withPart => {
    test(s"material view can't be update by common operator - with partition: $withPart") {
      withTempDir(f => {
        val viewPath = f.getCanonicalPath
        val viewName = "mv1"
        val sqlText = s"select * from star.$tableName1 a join star.$tableName2 b on a.k=b.key where a.k<3"

        if (withPart) {
          StarTable.createMaterialView(
            viewName,
            viewPath,
            sqlText,
            "p",
            "k",
            2,
            true)
        } else {
          StarTable.createMaterialView(
            viewName = viewName,
            viewPath = viewPath,
            sqlText = sqlText,
            autoUpdate = true)
        }

        checkAnswer(
          StarTable.forName(viewName).toDF.select("k", "v", "p", "key", "value", "partition"),
          Seq((1, "a", "1", 1, "aa", 11), (2, "b", "1", 2, "bb", 11)).toDF("k", "v", "p", "key", "value", "partition")
        )

        val e1 = intercept[AnalysisException] {
          sql(s"update star.$viewName set v='1a' where k=1")
        }
        assert(e1.getMessage().contains("Material view can't be changed by common insert/update/upsert"))

        val e2 = intercept[AnalysisException] {
          StarTable.forName(viewName).update(col("k").equalTo(1), Map("v" -> lit("1a")))
        }
        assert(e2.getMessage().contains("Material view can't be changed by common insert/update/upsert"))

        if (withPart) {
          val e3 = intercept[AnalysisException] {
            StarTable.forPath(viewPath).upsert(Seq((3, "c", "1", 3, "cc", 11)).toDF("k", "v", "p", "key", "value", "partition"))
          }
          assert(e3.getMessage().contains("Material view can't be changed by common insert/update/upsert"))
        } else {
          val e4 = intercept[AnalysisException] {
            sql(s"insert into star.$viewName (k,v,p,key,value,partition) values(3,'cc',11,3,'cc',22)")
          }
          assert(e4.getMessage().contains("Material view can't be changed by common insert/update/upsert"))
        }

      })

    }

  })

  Seq(true, false).foreach(withPart => {
    test(s"update material view - with partition: $withPart") {
      withTempDir(dir1 => {
        withTempDir(dir2 => {
          val viewPath = dir1.getCanonicalPath
          val viewName = "mv"
          val tablePath = dir2.getCanonicalPath
          val sqlText = s"select t1.k id,t1.v,t1.p,t2.name from star.$tableName1 t1 join star.`$tablePath` t2 on t1.k=t2.id"

          Seq((2, "bob"), (3, "saber"), (4, "lancer"), (5, "john")).toDF("id", "name")
            .write.mode("overwrite")
            .format("star")
            .option("hashPartitions", "id")
            .option("hashBucketNum", "2")
            .save(tablePath)

          if (withPart) {
            StarTable.createMaterialView(viewName, viewPath, sqlText, "p", "id", 2)
          } else {
            StarTable.createMaterialView(viewName, viewPath, sqlText)
          }

          checkAnswer(StarTable.forName(viewName).toDF.select("id", "v", "p", "name"),
            Seq((2, "b", "1", "bob"), (3, "c", "1", "saber"), (4, "d", "2", "lancer"))
              .toDF("id", "v", "p", "name"))

          //can't use updateMaterialView() if the table is not a material view
          val e1 = intercept[AnalysisException] {
            StarTable.forPath(tablePath).updateMaterialView()
          }
          assert(e1.getMessage().contains("is not material view"))

          //change table data
          StarTable.forPath(tablePath).upsert(Seq((1, "alina"), (2, "hana")).toDF("id", "name"))

          //can't read material view with stale data by default
          val e2 = intercept[AnalysisException] {
            withSQLConf(StarLakeSQLConf.ALLOW_STALE_MATERIAL_VIEW.key -> "false") {
              StarTable.forName(viewName).toDF.show()
            }
          }
          assert(e2.getMessage().contains("is staled, please update this view before read"))

          //set spark.engineplus.star.allow.stale.materialView = true to enable reading stale data from material view
          withSQLConf(StarLakeSQLConf.ALLOW_STALE_MATERIAL_VIEW.key -> "true") {
            StarTable.forName(viewName).toDF.show()
          }

          //get material info before update, and all tables' partition version should be 1
          val relationTables1 = MaterialView.getMaterialViewInfo(viewName).get
            .relationTables.map(t => (t.tableName, t)).toMap
          val tables = relationTables1.keySet

          assert(tables.forall(t => {
            relationTables1(t).partitionInfo.forall(p => p._2.toLong == 1)
          }))

          StarTable.forName(viewName).updateMaterialView()
          checkAnswer(StarTable.forName(viewName).toDF.select("id", "v", "p", "name"),
            Seq((1, "a", "1", "alina"), (2, "b", "1", "hana"), (3, "c", "1", "saber"), (4, "d", "2", "lancer"))
              .toDF("id", "v", "p", "name"))

          val changeTableName = MetaUtils.modifyTableString(tablePath)
          //get material info after update
          val relationTables2 = MaterialView.getMaterialViewInfo(viewName).get
            .relationTables.map(t => (t.tableName, t)).toMap
          assert(tables.forall(t => {
            if (t.equals(changeTableName)) {
              relationTables2(t).partitionInfo.forall(p => p._2.toLong == 2)
            } else {
              relationTables2(t).partitionInfo.forall(p => p._2.toLong == 1)
            }
          }))

          //update will not change anything
          StarTable.forName(viewName).updateMaterialView()

          val relationTables3 = MaterialView.getMaterialViewInfo(viewName).get
            .relationTables.map(t => (t.tableName, t)).toMap

          tables.foreach(t => {
            assert(relationTables2(t).toString.equals(relationTables3(t).toString))
          })


        })
      })
    }
  })


  test("drop table will also drop material views associated with it") {
    withTempDir(dir1 => {
      withTempDir(dir2 => {
        val viewPath = dir1.getCanonicalPath
        val viewName = "mv"
        val tablePath = dir2.getCanonicalPath
        val sqlText = s"select t1.k id,t1.v,t1.p,t2.name from star.$tableName1 t1 join star.`$tablePath` t2 on t1.k=t2.id"

        Seq((2, "bob"), (3, "saber"), (4, "lancer"), (5, "john")).toDF("id", "name")
          .write.mode("overwrite")
          .format("star")
          .option("hashPartitions", "id")
          .option("hashBucketNum", "2")
          .save(tablePath)

        StarTable.createMaterialView(viewName, viewPath, sqlText)

        val tableInfo = SnapshotManagement(tablePath).getTableInfoOnly
        val viewInfo = SnapshotManagement(viewPath).getTableInfoOnly

        assert(MaterialView.getMaterialViewInfo(viewName).isDefined)
        assert(MaterialView.getMaterialRelationInfo(tableInfo.table_id).nonEmpty)
        assert(MetaVersion.isTableExists(viewInfo.table_name))

        StarTable.forPath(tablePath).dropTable()

        assert(MaterialView.getMaterialViewInfo(viewName).isEmpty)
        assert(MaterialView.getMaterialRelationInfo(tableInfo.table_id).isEmpty)
        assert(!MetaVersion.isTableExists(viewInfo.table_name))

      })
    })
  }

  test("create material view with non-star table should failed") {
    withTempDir(dir1 => {
      val viewPath = dir1.getCanonicalPath
      val viewName = "mv"

      Seq((2, "bob"), (3, "saber"), (4, "lancer"), (5, "john")).toDF("id", "name")
        .createOrReplaceTempView("tmpTable")

      val sqlText = s"select t1.k id,t1.v,t1.p,t2.name from star.$tableName1 t1 join tmpTable t2 on t1.k=t2.id"


      val e = intercept[AnalysisException] {
        StarTable.createMaterialView(viewName, viewPath, sqlText)
      }

      assert(e.getMessage().contains("Material view can only build with star table, but non-star table was found"))

    })
  }


}
