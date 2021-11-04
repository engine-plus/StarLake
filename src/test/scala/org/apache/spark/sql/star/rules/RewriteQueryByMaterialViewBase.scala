package org.apache.spark.sql.star.rules

import com.engineplus.star.tables.StarTable
import org.apache.spark.sql.{AnalysisException, QueryTest}
import org.apache.spark.sql.star.test.StarLakeSQLCommandTest
import org.apache.spark.sql.test.SharedSparkSession
import org.scalatest.BeforeAndAfterAll
import org.apache.spark.util.Utils

abstract class RewriteQueryByMaterialViewBase extends QueryTest
  with SharedSparkSession with StarLakeSQLCommandTest with BeforeAndAfterAll {

  import testImplicits._

  val tableName1: String = "tmp_table1"
  val tablePath1: String = Utils.createTempDir().getCanonicalPath
  val tablePath2: String = Utils.createTempDir().getCanonicalPath
  val tablePath3: String = Utils.createTempDir().getCanonicalPath





  //  val viewName: String = "material_view"
  //  val viewPath: String = Utils.createTempDir().getCanonicalPath


  def prepareTable1(): Unit ={
    Seq((1,"a",11),(2,"b",11),(3,"c",11),(4,"d",11)).toDF("key","value","range")
      .write
      .mode("overwrite")
      .format("star")
      .option("rangePartitions", "range")
      .option("hashPartitions", "key")
      .option("hashBucketNum", "2")
      .option("shortTableName", tableName1)
      .save(tablePath1)
  }

  def prepareTable2(): Unit = {
    Seq((1,"aa",22),(3,"cc",22),(5,"ee",22),(6,"ff",22)).toDF("key","value","range")
      .write
      .mode("overwrite")
      .format("star")
      .save(tablePath2)
  }

  def prepareTable3(): Unit = {
    Seq(
      (1,"a","a2",22),(3,"c","c2",22),(5,"e","e2",22),(6,"f","f2",22),
      (1,"a","a3",33),(4,"d","d3",33),(5,"e","e3",33),(6,"f","f3",33)
    ).toDF("k1","k2","value","range")
      .write
      .mode("overwrite")
      .option("rangePartitions", "range")
      .format("star")
      .save(tablePath3)
  }

  def prepareMaterialViews(): Unit

  def cleanMaterialViews(): Unit

  def dropTable(tablePath: String): Unit = {
    try {
      StarTable.forPath(tablePath).dropTable()
    } catch {
      case e: AnalysisException
        if e.getMessage().contains("Table") && e.getMessage().contains("doesn't exist") =>

      case e => throw e
    }

  }


  override def beforeAll(){

    super.beforeAll()
    prepareTable1()
    prepareTable2()
    prepareTable3()

    prepareMaterialViews()

  }


  override def afterAll(){
    StarTable.forPath(tablePath1).dropTable()
    StarTable.forPath(tablePath2).dropTable()
    StarTable.forPath(tablePath3).dropTable()

    cleanMaterialViews()
    super.afterAll()
  }

}

class SimpleRewriteWithSingleTable extends RewriteQueryByMaterialViewBase {
  import testImplicits._

  val viewName1: String = "material_view1"
  val viewPath1: String = Utils.createTempDir().getCanonicalPath

  override def prepareMaterialViews(): Unit = {
    val sqlText1 =
      s"""
         |select a.key,value,range,length(range) as lr,concat_ws(',',key,a.value,'something') as cw
         |from star.`$tablePath1` a
         |where range>=5 and range<30
       """.stripMargin
    StarTable.createMaterialView(
      viewName1,
      viewPath1,
      sqlText1
    )
  }

  override def cleanMaterialViews(): Unit = {
    dropTable(viewPath1)
  }


  test("same query will be rewritten by material view"){
    val sqlText =
      s"""
         |select a.key,value,range,length(range) as lr,concat_ws(',',key,a.value,'something') as cw
         |from star.`$tablePath1` a
         |where range>=5 and range<30
       """.stripMargin
    val query = spark.sql(sqlText)

    val plan = query.queryExecution.optimizedPlan.toString()
    assert(plan.contains(viewPath1))

    checkAnswer(query.select("key", "value", "range", "lr", "cw"),
      Seq(
        (1,"a",11,2,"1,a,something"),
        (2,"b",11,2,"2,b,something"),
        (3,"c",11,2,"3,c,something"),
        (4,"d",11,2,"4,d,something"))
        .toDF("key", "value", "range", "lr", "cw"))

  }


  test("query with short table name will be rewritten by material view"){
    val sqlText =
      s"""
         |select a.key,value,range,length(range) as lr,concat_ws(',',key,a.value,'something') as cw
         |from star.`$tableName1` a
         |where range>=5 and range<30
       """.stripMargin
    val query = spark.sql(sqlText)

    val plan = query.queryExecution.optimizedPlan.toString()
    assert(plan.contains(viewPath1))
  }

  test("same query with range condition both sides exchanged will be rewritten by material view"){
    val sqlText =
      s"""
         |select a.key,value,range,length(range) as lr,concat_ws(',',key,a.value,'something') as cw
         |from star.`$tablePath1` a
         |where 5<=range and 30>range
       """.stripMargin
    val query = spark.sql(sqlText)

    val plan = query.queryExecution.optimizedPlan.toString()
    assert(plan.contains(viewPath1))

    checkAnswer(query.select("key", "value", "range", "lr", "cw"),
      Seq(
        (1,"a",11,2,"1,a,something"),
        (2,"b",11,2,"2,b,something"),
        (3,"c",11,2,"3,c,something"),
        (4,"d",11,2,"4,d,something"))
        .toDF("key", "value", "range", "lr", "cw"))

  }


  test("same query with different column name will be rewritten by material view"){
    val sqlText =
      s"""
         |select a.key,value,range,length(range) as cw,concat_ws(',',key,a.value,'something') as lr
         |from star.`$tablePath1` a
         |where range>=5 and range<30
       """.stripMargin
    val query = spark.sql(sqlText)

    val plan = query.queryExecution.optimizedPlan.toString()
    assert(plan.contains(viewPath1))

    checkAnswer(query.select("key", "value", "range", "cw", "lr"),
      Seq(
        (1,"a",11,2,"1,a,something"),
        (2,"b",11,2,"2,b,something"),
        (3,"c",11,2,"3,c,something"),
        (4,"d",11,2,"4,d,something"))
        .toDF("key", "value", "range", "cw", "lr"))

  }

  test("query with large range interval shouldn't rewrite"){
    val sqlText1 =
      s"""
         |select key
         |from star.`$tablePath1` a
         |where range>=5
       """.stripMargin
    val query1 = spark.sql(sqlText1)
    val plan1 = query1.queryExecution.optimizedPlan.toString()
    assert(!plan1.contains(viewPath1))

    val sqlText2 =
      s"""
         |select key
         |from star.`$tablePath1` a
         |where range>=5 and range<=30
       """.stripMargin
    val query2 = spark.sql(sqlText2)
    val plan2 = query2.queryExecution.optimizedPlan.toString()
    assert(!plan2.contains(viewPath1))

  }


  test("query with subset range interval should rewrite"){
    val sqlText1 =
      s"""
         |select key
         |from star.`$tablePath1` a
         |where range>5 and range<30
       """.stripMargin
    val query1 = spark.sql(sqlText1)
    val plan1 = query1.queryExecution.optimizedPlan.toString()
    assert(plan1.contains(viewPath1))
    checkAnswer(query1.select("key"),
      Seq(
        (1,"a",11,2,"1,a,something"),(2,"b",11,2,"2,b,something"),
        (3,"c",11,2,"3,c,something"),(4,"d",11,2,"4,d,something"))
        .toDF("key", "value", "range", "lr", "cw")
        .select("key"))


    val sqlText2 =
      s"""
         |select key
         |from star.`$tablePath1` a
         |where range>7 and range<=20
       """.stripMargin
    val query2 = spark.sql(sqlText2)
    val plan2 = query2.queryExecution.optimizedPlan.toString()
    assert(plan2.contains(viewPath1))
    checkAnswer(query2.select("key"),
      Seq(
        (1,"a",11,2,"1,a,something"),(2,"b",11,2,"2,b,something"),
        (3,"c",11,2,"3,c,something"),(4,"d",11,2,"4,d,something"))
        .toDF("key", "value", "range", "lr", "cw")
        .select("key"))
  }


  test("query with external condition should rewrite"){
    val sqlText1 =
      s"""
         |select key
         |from star.`$tablePath1` a
         |where range>5 and range<30 and key > 2
       """.stripMargin
    val query1 = spark.sql(sqlText1)
    val plan1 = query1.queryExecution.optimizedPlan.toString()
    assert(plan1.contains(viewPath1))
    checkAnswer(query1.select("key"),
      Seq(
        (3,"c",11,2,"3,c,something"),(4,"d",11,2,"4,d,something"))
        .toDF("key", "value", "range", "lr", "cw")
        .select("key"))


    val sqlText2 =
      s"""
         |select key
         |from star.`$tablePath1` a
         |where range>7 and range<=20 and (value='b' or value='d')
       """.stripMargin
    val query2 = spark.sql(sqlText2)
    val plan2 = query2.queryExecution.optimizedPlan.toString()
    assert(plan2.contains(viewPath1))
    checkAnswer(query2.select("key"),
      Seq(
        (2,"b",11,2,"2,b,something"),
        (4,"d",11,2,"4,d,something"))
        .toDF("key", "value", "range", "lr", "cw")
        .select("key"))
  }

  test("query with external or condition will be rewritten by material view"){
    val sqlText =
      s"""
         |select a.key,value,range,length(range) as lr,concat_ws(',',key,a.value,'something') as cw
         |from star.`$tablePath1` a
         |where range>=5 and range<30 and (key=1 or key>=3)
       """.stripMargin
    val query = spark.sql(sqlText)

    val plan = query.queryExecution.optimizedPlan.toString()
    assert(plan.contains(viewPath1))

    checkAnswer(query.select("key", "value", "range", "lr", "cw"),
      Seq(
        (1,"a",11,2,"1,a,something"),
        (3,"c",11,2,"3,c,something"),
        (4,"d",11,2,"4,d,something"))
        .toDF("key", "value", "range", "lr", "cw"))

  }
}


class RewriteWithJoinCondition extends RewriteQueryByMaterialViewBase {

  import testImplicits._


  val viewName2: String = "material_view2"
  val viewPath2: String = Utils.createTempDir().getCanonicalPath

  val viewName5: String = "material_view5"
  val viewPath5: String = Utils.createTempDir().getCanonicalPath


  override def prepareMaterialViews(): Unit = {
    val sqlText2 =
      s"""
         |select a.key as a_key,a.value a_value,a.range a_range,b.key b_key,b.value b_value,b.range b_range,
         |length(b.range) as lr,
         |concat_ws(',',a.key,a.value,'something') as cw
         |from star.`$tablePath1` a join star.`$tablePath2` b on a.key=b.key
         |where a.range>=2 and b.range<30 and b.value='cc'
       """.stripMargin
    StarTable.createMaterialView(
      viewName2,
      viewPath2,
      sqlText2
    )

    val sqlText5 =
      s"""
         |select a.key as a_key,a.value a_value,a.range a_range,
         |b.t1_value b_t1_value,b.t2_value,b.range b_range
         |from star.`$tablePath1` a join
         |(select t1.key,t2.k2,t1.value t1_value,t2.value t2_value,t2.range range
         | from star.`$tablePath2` t1 join
         |  (select * from star.`$tablePath3` where range>10) t2
         | on t1.key=t2.k1) b
         |on a.key=b.key
         |where a.range>=5
       """.stripMargin
    StarTable.createMaterialView(
      viewName5,
      viewPath5,
      sqlText5
    )


  }

  override def cleanMaterialViews(): Unit = {
    dropTable(viewPath2)
    dropTable(viewPath5)

  }

  test("same query should rewrite - join"){
    val sqlText =
      s"""
         |select a.key as a_key,a.value a_value,a.range a_range,b.key b_key,b.value b_value,b.range b_range,
         |length(b.range) as lr,
         |concat_ws(',',a.key,a.value,'something') as cw
         |from star.`$tablePath1` a join star.`$tablePath2` b on a.key=b.key
         |where a.range>=2 and b.range<30 and b.value='cc'
       """.stripMargin
    val query = spark.sql(sqlText)

    val plan = query.queryExecution.optimizedPlan.toString()
    assert(plan.contains(viewPath2))

    checkAnswer(query.select("a_key", "a_value", "a_range", "b_key", "b_value", "b_range", "lr", "cw"),
      Seq(
        (3,"c",11,3,"cc",22,2,"3,c,something"))
        .toDF("a_key", "a_value", "a_range", "b_key", "b_value", "b_range", "lr", "cw"))

  }

  test("query with external condition should rewrite - join"){
    val sqlText =
      s"""
         |select a.key as a_key,a.value a_value,a.range a_range,b.key b_key,b.value b_value,b.range b_range,
         |length(b.range) as lr,
         |concat_ws(',',a.key,a.value,'something') as cw
         |from star.`$tablePath1` a join star.`$tablePath2` b on a.key=b.key
         |where a.range>2 and b.range<25 and b.value='cc' and a.value='c'
       """.stripMargin
    val query = spark.sql(sqlText)

    val plan = query.queryExecution.optimizedPlan.toString()
    assert(plan.contains(viewPath2))

    checkAnswer(query.select("a_key", "a_value", "a_range", "b_key", "b_value", "b_range", "lr", "cw"),
      Seq(
        (3,"c",11,3,"cc",22,2,"3,c,something"))
        .toDF("a_key", "a_value", "a_range", "b_key", "b_value", "b_range", "lr", "cw"))
  }

  test("query with less condition should not rewrite - join"){
    val sqlText =
      s"""
         |select a.key as a_key,a.value a_value,a.range a_range,b.key b_key,b.value b_value,b.range b_range,
         |length(b.range) as lr,
         |concat_ws(',',a.key,a.value,'something') as cw
         |from star.`$tablePath1` a join star.`$tablePath2` b on a.key=b.key
         |where a.range>2 and b.range<25
       """.stripMargin
    val query = spark.sql(sqlText)

    val plan = query.queryExecution.optimizedPlan.toString()
    assert(!plan.contains(viewPath2))
  }



  test("same query should rewrite - multi table inner join"){
    val sqlText =
      s"""
         |select a.key as a_key,a.value a_value,a.range a_range,
         |b.t1_value b_t1_value,b.t2_value,b.range b_range
         |from star.`$tablePath1` a join
         |(select t1.key,t2.k2,t1.value t1_value,t2.value t2_value,t2.range range
         | from star.`$tablePath2` t1 join
         |  (select * from star.`$tablePath3` where range>10) t2
         | on t1.key=t2.k1) b
         |on a.key=b.key
         |where a.range>=5
       """.stripMargin

    val query = spark.sql(sqlText)

    val plan = query.queryExecution.optimizedPlan.toString()
    assert(plan.contains(viewPath5))

    checkAnswer(query.select("a_key", "a_value", "a_range", "b_t1_value","t2_value","b_range"),
      Seq(
        (1, "a", 11, "aa","a2",22),
        (1, "a", 11, "aa","a3",33),
        (3, "c", 11, "cc","c2",22))
        .toDF("a_key", "a_value", "a_range", "b_t1_value","t2_value","b_range"))
  }

  test("query with external condition in `on` should rewrite - multi table inner join"){
    val sqlText =
      s"""
         |select a.key as a_key,a.value a_value,a.range a_range,
         |b.t1_value b_t1_value,b.t2_value,b.range b_range
         |from star.`$tablePath1` a join
         |(select t1.key,t2.k2,t1.value t1_value,t2.value t2_value,t2.range range
         | from star.`$tablePath2` t1 join
         |  (select * from star.`$tablePath3` where range>10) t2
         | on t1.key=t2.k1 and key<10) b
         |on a.key=b.key and b.t2_value!='a2'
         |where a.range>=5
       """.stripMargin

    val query = spark.sql(sqlText)

    val plan = query.queryExecution.optimizedPlan.toString()
    assert(plan.contains(viewPath5))

    checkAnswer(query.select("a_key", "a_value", "a_range", "b_t1_value","t2_value","b_range"),
      Seq(
        (1, "a", 11, "aa","a3",33),
        (3, "c", 11, "cc","c2",22))
        .toDF("a_key", "a_value", "a_range", "b_t1_value","t2_value","b_range"))
  }

  test("query with less condition shouldn't rewrite - multi table inner join"){
    val sqlText =
      s"""
         |select a.key as a_key,a.value a_value,a.range a_range,
         |b.t1_value b_t1_value,b.t2_value,b.range b_range
         |from star.`$tablePath1` a join
         |(select t1.key,t2.k2,t1.value t1_value,t2.value t2_value,t2.range range
         | from star.`$tablePath2` t1 join
         |  (select * from star.`$tablePath3` where range>10) t2
         | on t1.key=t2.k1) b
         |on a.key=b.key
       """.stripMargin

    val query = spark.sql(sqlText)

    val plan = query.queryExecution.optimizedPlan.toString()
    assert(!plan.contains(viewPath5))
  }


}


class RewriteWithAggregateCondition extends RewriteQueryByMaterialViewBase {

  import testImplicits._


  val viewName3: String = "material_view3"
  val viewPath3: String = Utils.createTempDir().getCanonicalPath

  val viewName4: String = "material_view4"
  val viewPath4: String = Utils.createTempDir().getCanonicalPath

  val viewName6: String = "material_view6"
  val viewPath6: String = Utils.createTempDir().getCanonicalPath


  override def prepareMaterialViews(): Unit = {
    val sqlText3 =
      s"""
         |select a.k1 a_k1,a.k2 a_k2,collect_list(a.value) a_value,max(a.range) a_range
         |from star.`$tablePath3` a
         |where a.range>=2
         |group by a.k1,a.k2 having a.k1 > 1
       """.stripMargin
    StarTable.createMaterialView(
      viewName3,
      viewPath3,
      sqlText3
    )

    val sqlText4 =
      s"""
         |select a.key as a_key,max(a.value) a_value,last(a.range) a_range,min(b.value) b_value
         |from star.`$tablePath1` a left join star.`$tablePath2` b on a.key=b.key
         |where a.range>=2
         |group by a.key having a.key > 1
       """.stripMargin
    StarTable.createMaterialView(
      viewName4,
      viewPath4,
      sqlText4
    )

    val sqlText6 =
      s"""
         |select max(a.key) as a_key,min(a.value) a_value,first(a.range) a_range,
         |last(b.value) b_value,last(b.range) b_range,
         |c.k1,c.k2,collect_list(c.value) c_value,collect_list(c.range) c_range
         |from star.`$tablePath1` a,star.`$tablePath2` b,star.`$tablePath3` c
         |where a.key=b.key and b.key=c.k1 and a.range>=5 and c.range>10
         |group by c.k1,c.k2
       """.stripMargin
    StarTable.createMaterialView(
      viewName6,
      viewPath6,
      sqlText6
    )

  }

  override def cleanMaterialViews(): Unit = {
    dropTable(viewPath3)
    dropTable(viewPath4)
    dropTable(viewPath6)

  }


  test("same query should rewrite - aggregate"){
    val sqlText =
      s"""
         |select a.k1 a_k1,a.k2 a_k2,collect_list(a.value) a_value,max(a.range) a_range
         |from star.`$tablePath3` a
         |where a.range>=2
         |group by a.k1,a.k2 having a.k1 > 1
       """.stripMargin
    val query = spark.sql(sqlText)

    val plan = query.queryExecution.optimizedPlan.toString()
    assert(plan.contains(viewPath3))

    checkAnswer(query.select("a_k1", "a_k2", "a_value", "a_range"),
      Seq(
        (3, "c", List("c2"),22),
        (4, "d", List("d3"),33),
        (5, "e", List("e2","e3"),33),
        (6, "f", List("f2","f3"),33))
        .toDF("a_k1", "a_k2", "a_value", "a_range"))

  }

  test("query with external having condition should rewrite - aggregate"){
    val sqlText =
      s"""
         |select a.k1 a_k1,a.k2 a_k2,collect_list(a.value) a_value,max(a.range) a_range
         |from star.`$tablePath3` a
         |where a.range>=2
         |group by a.k1,a.k2 having a.k1 > 1 and a.k2='d'
       """.stripMargin
    val query = spark.sql(sqlText)

    val plan = query.queryExecution.optimizedPlan.toString()
    assert(plan.contains(viewPath3))

    checkAnswer(query.select("a_k1", "a_k2", "a_value", "a_range"),
      Seq(
        (4, "d", List("d3"),33))
        .toDF("a_k1", "a_k2", "a_value", "a_range"))
  }

  test("query with external condition under aggregate shouldn't rewrite - aggregate"){
    val sqlText =
      s"""
         |select a.k1 a_k1,a.k2 a_k2,collect_list(a.value) a_value,max(a.range) a_range
         |from star.`$tablePath3` a
         |where a.range>=2 and a.k2='d'
         |group by a.k1,a.k2 having a.k1 > 1
       """.stripMargin
    val query = spark.sql(sqlText)

    val plan = query.queryExecution.optimizedPlan.toString()
    assert(!plan.contains(viewPath3))
  }

  test("same query should rewrite - aggregate & left join"){
    val sqlText =
      s"""
         |select a.key as a_key,max(a.value) a_value,last(a.range) a_range,min(b.value) b_value
         |from star.`$tablePath1` a left join star.`$tablePath2` b on a.key=b.key
         |where a.range>=2
         |group by a.key having a.key > 1
       """.stripMargin

    val query = spark.sql(sqlText)

    val plan = query.queryExecution.optimizedPlan.toString()
    assert(plan.contains(viewPath4))

    checkAnswer(query.select("a_key", "a_value", "a_range", "b_value"),
      Seq(
        (2, "b", 11, null),
        (3, "c", 11, "cc"),
        (4, "d", 11, null))
        .toDF("a_key", "a_value", "a_range", "b_value"))
  }

  test("query with external having condition should rewrite - aggregate & left join"){
    val sqlText =
      s"""
         |select a.key as a_key,max(a.value) a_value,last(a.range) a_range,min(b.value) b_value
         |from star.`$tablePath1` a left join star.`$tablePath2` b on a.key=b.key
         |where a.range>=2
         |group by a.key having a.key>1 and min(b.value)='cc'
       """.stripMargin

    val query = spark.sql(sqlText)

    val plan = query.queryExecution.optimizedPlan.toString()
    assert(plan.contains(viewPath4))

    checkAnswer(query.select("a_key", "a_value", "a_range", "b_value"),
      Seq(
        (3, "c", 11, "cc"))
        .toDF("a_key", "a_value", "a_range", "b_value"))
  }


  test("query with external condition under aggregate shouldn't rewrite - aggregate & left join"){
    val sqlText =
      s"""
         |select a.key as a_key,max(a.value) a_value,last(a.range) a_range,min(b.value) b_value
         |from star.`$tablePath1` a left join star.`$tablePath2` b on a.key=b.key
         |where a.range>=2 and b.value='c'
         |group by a.key having a.key>1
       """.stripMargin

    val query = spark.sql(sqlText)

    val plan = query.queryExecution.optimizedPlan.toString()
    assert(!plan.contains(viewPath4))
  }

  test("query with external join condition under aggregate shouldn't rewrite - aggregate & left join"){
    val sqlText =
      s"""
         |select a.key as a_key,max(a.value) a_value,last(a.range) a_range,min(b.value) b_value
         |from star.`$tablePath1` a left join star.`$tablePath2` b on a.key=b.key and b.value='c'
         |where a.range>=2
         |group by a.key having a.key>1
       """.stripMargin

    val query = spark.sql(sqlText)

    val plan = query.queryExecution.optimizedPlan.toString()
    assert(!plan.contains(viewPath4))
  }


  test("same query should rewrite - aggregate & multi join"){
    val sqlText =
      s"""
         |select max(a.key) as a_key,min(a.value) a_value,first(a.range) a_range,
         |last(b.value) b_value,last(b.range) b_range,
         |c.k1,c.k2,collect_list(c.value) c_value,collect_list(c.range) c_range
         |from star.`$tablePath1` a,star.`$tablePath2` b,star.`$tablePath3` c
         |where a.key=b.key and b.key=c.k1 and a.range>=5 and c.range>10
         |group by c.k1,c.k2
       """.stripMargin

    val query = spark.sql(sqlText)

    val plan = query.queryExecution.optimizedPlan.toString()
    assert(plan.contains(viewPath6))

    checkAnswer(query.select("a_key", "a_value", "a_range", "b_value","b_range","k1", "k2","c_value","c_range"),
      Seq(
        (1, "a", 11, "aa",22,1,"a",List("a3","a2"),List(33,22)),
        (3, "c", 11, "cc",22,3,"c",List("c2"),List(22)))
        .toDF("a_key", "a_value", "a_range", "b_value","b_range","k1", "k2","c_value","c_range"))
  }

  test("query with having condition should rewrite - aggregate & multi join"){
    val sqlText =
      s"""
         |select max(a.key) as a_key,min(a.value) a_value,first(a.range) a_range,
         |last(b.value) b_value,last(b.range) b_range,
         |c.k1,c.k2,collect_list(c.value) c_value,collect_list(c.range) c_range
         |from star.`$tablePath1` a,star.`$tablePath2` b,star.`$tablePath3` c
         |where a.key=b.key and b.key=c.k1 and a.range>=5 and c.range>10
         |group by c.k1,c.k2
         |having min(a.value)='c'
       """.stripMargin

    val query = spark.sql(sqlText)

    val plan = query.queryExecution.optimizedPlan.toString()
    assert(plan.contains(viewPath6))

    checkAnswer(query.select("a_key", "a_value", "a_range", "b_value","b_range","k1", "k2","c_value","c_range"),
      Seq(
        (3, "c", 11, "cc",22,3,"c",List("c2"),List(22)))
        .toDF("a_key", "a_value", "a_range", "b_value","b_range","k1", "k2","c_value","c_range"))
  }


  test("query with external condition under aggregate shouldn't rewrite - aggregate & multi join"){
    val sqlText =
      s"""
         |select max(a.key) as a_key,min(a.value) a_value,first(a.range) a_range,
         |last(b.value) b_value,last(b.range) b_range,
         |c.k1,c.k2,collect_list(c.value) c_value,collect_list(c.range) c_range
         |from star.`$tablePath1` a,star.`$tablePath2` b,star.`$tablePath3` c
         |where a.key=b.key and b.key=c.k1 and a.range>=5 and c.range>10 and a.value='c'
         |group by c.k1,c.k2
       """.stripMargin

    val query = spark.sql(sqlText)

    val plan = query.queryExecution.optimizedPlan.toString()
    assert(!plan.contains(viewPath6))
  }
}



class RewriteWithOrCondition extends RewriteQueryByMaterialViewBase {

  import testImplicits._



  val viewName7: String = "material_view7"
  val viewPath7: String = Utils.createTempDir().getCanonicalPath

  val viewName8: String = "material_view8"
  val viewPath8: String = Utils.createTempDir().getCanonicalPath


  override def prepareMaterialViews(): Unit = {
    val sqlText7 =
      s"""
         |select a.k1,k2,value,range,length(range) as lr,concat_ws(',',k1,k2,a.value) as cw
         |from star.`$tablePath3` a
         |where (k1>1 and range>=30) or (k1<=3 and range<25 and value!='a2') or k1=5
       """.stripMargin
    StarTable.createMaterialView(
      viewName7,
      viewPath7,
      sqlText7
    )

    val sqlText8 =
      s"""
         |select a.k1,k2,value,range,length(range) as lr
         |from star.`$tablePath3` a
         |where ((k1>1 and range>=30) or (k1<=3 and range<25 and value!='a2') or k1=1) and k1>=0
       """.stripMargin
    StarTable.createMaterialView(
      viewName8,
      viewPath8,
      sqlText8
    )


  }

  override def cleanMaterialViews(): Unit = {
    dropTable(viewPath7)
    dropTable(viewPath8)

  }

  test("same query should rewrite - or"){
    val sqlText =
      s"""
         |select a.k1,k2,value,range,length(range) as lr,concat_ws(',',k1,k2,a.value) as cw
         |from star.`$tablePath3` a
         |where (k1>1 and range>=30) or (k1<=3 and range<25 and value!='a2') or k1=5
       """.stripMargin

    val query = spark.sql(sqlText)

    val plan = query.queryExecution.optimizedPlan.toString()
    assert(plan.contains(viewPath7))

    checkAnswer(query.select("k1", "k2", "value", "range","lr","cw"),
      Seq(
        (3, "c", "c2", 22, 2, "3,c,c2"),
        (4, "d", "d3", 33, 2, "4,d,d3"),
        (5, "e", "e2", 22, 2, "5,e,e2"),
        (5, "e", "e3", 33, 2, "5,e,e3"),
        (6, "f", "f3", 33, 2, "6,f,f3"))
        .toDF("k1", "k2", "value", "range","lr","cw"))
  }

  test("query with less or condition should rewrite - or"){
    val sqlText =
      s"""
         |select a.k1,k2,value,range,length(range) as lr,concat_ws(',',k1,k2,a.value) as cw
         |from star.`$tablePath3` a
         |where (k1>1 and range>=30) or k1=5
       """.stripMargin

    val query = spark.sql(sqlText)

    val plan = query.queryExecution.optimizedPlan.toString()
    assert(plan.contains(viewPath7))

    checkAnswer(query.select("k1", "k2", "value", "range","lr","cw"),
      Seq(
        (4, "d", "d3", 33, 2, "4,d,d3"),
        (5, "e", "e2", 22, 2, "5,e,e2"),
        (5, "e", "e3", 33, 2, "5,e,e3"),
        (6, "f", "f3", 33, 2, "6,f,f3"))
        .toDF("k1", "k2", "value", "range","lr","cw"))
  }


  test("query with or condition inbounds should rewrite - or"){
    val sqlText =
      s"""
         |select a.k1,k2,value,range,length(range) as lr,concat_ws(',',k1,k2,a.value) as cw
         |from star.`$tablePath3` a
         |where (k1>2 and range>30 and value='e3') or (k1<3 and range<24 and value!='a2')
       """.stripMargin

    val query = spark.sql(sqlText)

    val plan = query.queryExecution.optimizedPlan.toString()
    assert(plan.contains(viewPath7))

    checkAnswer(query.select("k1", "k2", "value", "range","lr","cw"),
      Seq(
        (5, "e", "e3", 33, 2, "5,e,e3"))
        .toDF("k1", "k2", "value", "range","lr","cw"))
  }


  test("query without or condition inbounds should rewrite - or"){
    val sqlText =
      s"""
         |select a.k1,k2,value,range,length(range) as lr,concat_ws(',',k1,k2,a.value) as cw
         |from star.`$tablePath3` a
         |where k1>1 and range>=30
       """.stripMargin

    val query = spark.sql(sqlText)

    val plan = query.queryExecution.optimizedPlan.toString()
    assert(plan.contains(viewPath7))

    checkAnswer(query.select("k1", "k2", "value", "range","lr","cw"),
      Seq(
        (4, "d", "d3", 33, 2, "4,d,d3"),
        (5, "e", "e3", 33, 2, "5,e,e3"),
        (6, "f", "f3", 33, 2, "6,f,f3"))
        .toDF("k1", "k2", "value", "range","lr","cw"))
  }



  test("query without or condition inbounds should rewrite (equal replace range) - or"){
    val sqlText =
      s"""
         |select a.k1,k2,value,range,length(range) as lr,concat_ws(',',k1,k2,a.value) as cw
         |from star.`$tablePath3` a
         |where k1=4 and range>=30
       """.stripMargin

    val query = spark.sql(sqlText)

    val plan = query.queryExecution.optimizedPlan.toString()
    assert(plan.contains(viewPath7))

    checkAnswer(query.select("k1", "k2", "value", "range","lr","cw"),
      Seq(
        (4, "d", "d3", 33, 2, "4,d,d3"))
        .toDF("k1", "k2", "value", "range","lr","cw"))
  }



  test("query with more or condition shouldn't rewrite - or"){
    val sqlText =
      s"""
         |select a.k1,k2,value,range,length(range) as lr,concat_ws(',',k1,k2,a.value) as cw
         |from star.`$tablePath3` a
         |where (k1>1 and range>=30) or (k1<=3 and range<25 and value!='a2') or k1=5 or k1=4
       """.stripMargin

    val query = spark.sql(sqlText)

    val plan = query.queryExecution.optimizedPlan.toString()
    println(query.queryExecution.toString())
    assert(!plan.contains(viewPath7))
  }

  test("query with or condition not inbounds shouldn't rewrite - or"){
    val sqlText =
      s"""
         |select a.k1,k2,value,range,length(range) as lr,concat_ws(',',k1,k2,a.value) as cw
         |from star.`$tablePath3` a
         |where (k1>0 and range>=30) or (k1<=3 and range<25 and value!='a2') or k1=5
       """.stripMargin

    val query = spark.sql(sqlText)

    val plan = query.queryExecution.optimizedPlan.toString()
    assert(!plan.contains(viewPath7))
  }


  test("query without or condition not inbounds shouldn't rewrite - or"){
    val sqlText =
      s"""
         |select a.k1,k2,value,range,length(range) as lr,concat_ws(',',k1,k2,a.value) as cw
         |from star.`$tablePath3` a
         |where k1=0 and range>=30
       """.stripMargin

    val query = spark.sql(sqlText)

    val plan = query.queryExecution.optimizedPlan.toString()
    assert(!plan.contains(viewPath7))
  }


  test("query without condition shouldn't rewrite - or"){
    val sqlText =
      s"""
         |select a.k1,k2,value,range,length(range) as lr,concat_ws(',',k1,k2,a.value) as cw
         |from star.`$tablePath3` a
       """.stripMargin

    val query = spark.sql(sqlText)

    val plan = query.queryExecution.optimizedPlan.toString()
    assert(!plan.contains(viewPath7))
  }


  test("same query should rewrite - or & and"){
    val sqlText =
      s"""
         |select a.k1,k2,value,range,length(range) as lr,concat_ws(',',k1,k2,a.value) as cw
         |from star.`$tablePath3` a
         |where ((k1>1 and range>=30) or (k1<=3 and range<25 and value!='a2') or k1=1) and k1>=0
       """.stripMargin

    val query = spark.sql(sqlText)

    val plan = query.queryExecution.optimizedPlan.toString()
    assert(plan.contains(viewPath8))

    checkAnswer(query.select("k1", "k2", "value", "range","lr","cw"),
      Seq(
        (1, "a", "a2", 22, 2, "1,a,a2"),
        (1, "a", "a3", 33, 2, "1,a,a3"),
        (3, "c", "c2", 22, 2, "3,c,c2"),
        (4, "d", "d3", 33, 2, "4,d,d3"),
        (5, "e", "e3", 33, 2, "5,e,e3"),
        (6, "f", "f3", 33, 2, "6,f,f3"))
        .toDF("k1", "k2", "value", "range","lr","cw"))

  }



  test("query with external condition should rewrite - or & and"){
    val sqlText =
      s"""
         |select a.k1,k2,value,range,length(range) as lr,concat_ws(',',k1,k2,a.value) as cw
         |from star.`$tablePath3` a
         |where ((k1<=3 and range<25 and value!='a2' and k2='c') or k1=1) and k1>=0
       """.stripMargin

    val query = spark.sql(sqlText)

    val plan = query.queryExecution.optimizedPlan.toString()
    assert(plan.contains(viewPath8))

    checkAnswer(query.select("k1", "k2", "value", "range","lr","cw"),
      Seq(
        (1, "a", "a2", 22, 2, "1,a,a2"),
        (1, "a", "a3", 33, 2, "1,a,a3"),
        (3, "c", "c2", 22, 2, "3,c,c2"))
        .toDF("k1", "k2", "value", "range","lr","cw"))
  }


  test("query without or condition inbounds should rewrite - or & and"){
    val sqlText =
      s"""
         |select a.k1,k2,value,range,length(range) as lr,concat_ws(',',k1,k2,a.value) as cw
         |from star.`$tablePath3` a
         |where k1=1
       """.stripMargin

    val query = spark.sql(sqlText)

    val plan = query.queryExecution.optimizedPlan.toString()
    assert(plan.contains(viewPath8))

    checkAnswer(query.select("k1", "k2", "value", "range","lr","cw"),
      Seq(
        (1, "a", "a2", 22, 2, "1,a,a2"),
        (1, "a", "a3", 33, 2, "1,a,a3"))
        .toDF("k1", "k2", "value", "range","lr","cw"))

  }

  test("query with or condition not inbounds shouldn't rewrite - or & and"){
    val sqlText =
      s"""
         |select a.k1,k2,value,range,length(range) as lr,concat_ws(',',k1,k2,a.value) as cw
         |from star.`$tablePath3` a
         |where ((k1>1 and range>=30) or (k1<=4 and range<25 and value!='a2') or k1=1) and k1>=0
       """.stripMargin

    val query = spark.sql(sqlText)

    val plan = query.queryExecution.optimizedPlan.toString()
    assert(!plan.contains(viewPath8))

  }

  test("query without or condition not inbounds shouldn't rewrite - or & and"){
    val sqlText =
      s"""
         |select a.k1,k2,value,range,length(range) as lr,concat_ws(',',k1,k2,a.value) as cw
         |from star.`$tablePath3` a
         |where k1=5
       """.stripMargin

    val query = spark.sql(sqlText)

    val plan = query.queryExecution.optimizedPlan.toString()
    assert(!plan.contains(viewPath8))

  }


  test("query without condition shouldn't rewrite - or & and"){
    val sqlText =
      s"""
         |select a.k1,k2,value,range,length(range) as lr,concat_ws(',',k1,k2,a.value) as cw
         |from star.`$tablePath3` a
       """.stripMargin

    val query = spark.sql(sqlText)

    val plan = query.queryExecution.optimizedPlan.toString()
    println(query.queryExecution.toString())
    assert(!plan.contains(viewPath8))

  }

  test("query without or condition (just and condition) shouldn't rewrite - or & and"){
    val sqlText =
      s"""
         |select a.k1,k2,value,range,length(range) as lr,concat_ws(',',k1,k2,a.value) as cw
         |from star.`$tablePath3` a
         |where k1>=0
       """.stripMargin

    val query = spark.sql(sqlText)

    val plan = query.queryExecution.optimizedPlan.toString()
    assert(!plan.contains(viewPath8))

  }




}

