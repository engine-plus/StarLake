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

package org.apache.spark.sql.star.commands

import java.io.File

import com.engineplus.star.tables.StarTableTestUtils
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.star.SnapshotManagement
import org.apache.spark.sql.star.sources.StarLakeSQLConf
import org.apache.spark.sql.star.test.StarLakeTestUtils
import org.apache.spark.sql.test.SharedSparkSession
import org.scalatest.BeforeAndAfterEach

class CleanupSuite extends QueryTest
  with SharedSparkSession with BeforeAndAfterEach
  with StarLakeTestUtils {

  import testImplicits._

  def writeData(tablePath: String): Unit = {
    spark.range(10).write.mode("overwrite").format("star").save(tablePath)
  }

  test("cleanup basic test") {
    withSQLConf(StarLakeSQLConf.OLD_VERSION_RETENTION_TIME.key -> "1") {
      withTempDir(tempDir => {
        val tablePath = tempDir.getCanonicalPath
        writeData(tablePath)
        val snapshotManagement = SnapshotManagement(tablePath)

        //add unrelated file manually
        val deleteDir = new File(tempDir.getAbsolutePath, "reservoir")
        assert(deleteDir.mkdirs())

        val fs = new Path(tablePath).getFileSystem(spark.sessionState.newHadoopConf())
        var result = CleanupCommand.runCleanup(spark, snapshotManagement)

        val deleteFile = fs.makeQualified(new Path(deleteDir.toString)).toString
        checkDatasetUnorderly(result.as[String], deleteFile)

        //clean old version expire files
        val dataInfo = snapshotManagement.updateSnapshot().allDataInfo
        var oldFiles = dataInfo.map(_.file_path) ++ Seq(deleteFile)
        val fileNum = dataInfo.length
        writeData(tablePath)
        result = CleanupCommand.runCleanup(spark, snapshotManagement)
        checkDatasetUnorderly(result.as[String], oldFiles: _*)

        oldFiles = snapshotManagement.updateSnapshot().allDataInfo.map(_.file_path) ++ oldFiles
        writeData(tablePath)
        result = CleanupCommand.runCleanup(spark, snapshotManagement)
        checkDatasetUnorderly(result.as[String], oldFiles: _*)

        oldFiles = snapshotManagement.updateSnapshot().allDataInfo.map(_.file_path) ++ oldFiles
        writeData(tablePath)
        result = CleanupCommand.runCleanup(spark, snapshotManagement)
        checkDatasetUnorderly(result.as[String], oldFiles: _*)


        oldFiles = snapshotManagement.updateSnapshot().allDataInfo.map(_.file_path) ++ oldFiles
        writeData(tablePath)
        result = CleanupCommand.runCleanup(spark, snapshotManagement)
        checkDatasetUnorderly(result.as[String], oldFiles: _*)

        CleanupCommand.runCleanup(spark, snapshotManagement, false)
        assert(
          StarTableTestUtils
            .getNumByTableId("data_info", snapshotManagement.snapshot.getTableInfo.table_id)
            == fileNum
        )

      })
    }

  }

}
