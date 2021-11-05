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

package org.apache.spark.sql.star.test

import java.io.File

import com.engineplus.star.sql.StarSparkSessionExtension
import com.engineplus.star.tables
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.star.catalog.StarLakeCatalog
import org.apache.spark.sql.star.sources.StarLakeSQLConf
import org.apache.spark.sql.test.{SharedSparkSession, TestSparkSession}
import org.apache.spark.sql.{SparkSession, SparkSessionExtensions}
import org.apache.spark.util.Utils


trait StarLakeTestUtils extends Logging {
  self: SharedSparkSession =>

  override protected def createSparkSession: TestSparkSession = {
    SparkSession.cleanupAnyExistingSession()
    val session = new StarLakeTestSparkSession(sparkConf)
    session.conf.set(StarLakeSQLConf.META_DATABASE_NAME.key, "test_star_meta")
    session
  }

  override def withTable(tableNames: String*)(f: => Unit): Unit = {
    Utils.tryWithSafeFinally(f) {
      tableNames.foreach { name =>
        spark.sql(s"DROP TABLE IF EXISTS $name")
        val starName = if (name.startsWith("star.")) name else s"star.$name"
        spark.sql(s"DROP TABLE IF EXISTS $starName")
      }
    }
  }

  override def withTempDir(f: File => Unit): Unit = {
    val dir = Utils.createTempDir()
    try {
      f(dir)
      waitForTasksToFinish()
    } finally {
      Utils.deleteRecursively(dir)
      try {
        tables.StarTable.forPath(dir.getCanonicalPath).dropTable()
      } catch {
        case e: Exception =>
      }
    }
  }
}

/**
  * Because `TestSparkSession` doesn't pick up the conf `spark.sql.extensions` in Spark 2.4.x, we use
  * this class to inject StarLake's extension in our tests.
  *
  * @see https://issues.apache.org/jira/browse/SPARK-25003
  */
class StarLakeTestSparkSession(sparkConf: SparkConf) extends TestSparkSession(sparkConf) {
  override val extensions: SparkSessionExtensions = {
    val extensions = new SparkSessionExtensions
    new StarSparkSessionExtension().apply(extensions)
    extensions
  }
}

/**
  * A trait for tests that are testing a fully set up SparkSession with all of StarLake's requirements,
  * such as the configuration of the StarLakeCatalog and the addition of all Star extensions.
  */
trait StarLakeSQLCommandTest extends StarLakeTestUtils {
  self: SharedSparkSession =>

  override protected def createSparkSession: TestSparkSession = {
    SparkSession.cleanupAnyExistingSession()
    val session = new StarLakeTestSparkSession(sparkConf)
    session.conf.set(StarLakeSQLConf.META_DATABASE_NAME.key, "test_star_meta")
    session.conf.set(SQLConf.V2_SESSION_CATALOG_IMPLEMENTATION.key, classOf[StarLakeCatalog].getName)

    session
  }
}

