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

import java.net.URI
import java.util.Date

import com.engineplus.star.meta.{DataOperation, MetaCommit}
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.star.sources.StarLakeSQLConf
import org.apache.spark.sql.star.utils.FileOperation
import org.apache.spark.sql.star.{SnapshotManagement, StarLakeUtils}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.util.{Clock, SerializableConfiguration, SystemClock}
import org.apache.spark.sql.functions._

import scala.collection.JavaConverters._

object CleanupCommand extends CleanupCommandImpl with Serializable{

  /**
    * Clears all untracked files and folders within this table. First lists all the files and
    * directories in the table, and gets the relative paths with respect to the base of the
    * table. Then it gets the list of all tracked files for this table, which may or may not
    * be within the table base path, and gets the relative paths of all the tracked files with
    * respect to the base of the table. Files outside of the table path will be ignored.
    * Then we take a diff of the files and delete directories that were already empty, and all files
    * that are within the table that are no longer tracked.
    *
    * @param dryRun If set to true, no files will be deleted. Instead, we will list all files and
    *               directories that will be cleared.
    * @return A Dataset containing the paths of the files/folders to delete in dryRun mode. Otherwise
    *         returns the base path of the table.
    */
  def runCleanup(spark: SparkSession,
                 snapshotManagement: SnapshotManagement,
                 dryRun: Boolean = true,
                 clock: Clock = new SystemClock): DataFrame = {
    val retentionMillis = spark.conf.get(StarLakeSQLConf.OLD_VERSION_RETENTION_TIME)
    val deleteBeforeTimestamp = clock.getTimeMillis() - retentionMillis

    MetaCommit.cleanUndoLog(snapshotManagement.getTableInfoOnly.table_id)
    snapshotManagement.updateSnapshot().getPartitionInfoArray.foreach(partitionInfo => {
      DataOperation.removeFileByName(
        partitionInfo.table_id,
        partitionInfo.range_id,
        partitionInfo.read_version)
    })

    val path = new Path(snapshotManagement.table_name)
    val sessionHadoopConf = spark.sessionState.newHadoopConf()
    val fs = path.getFileSystem(sessionHadoopConf)

    import spark.implicits._


    logInfo(s"Starting garbage collection (dryRun = $dryRun) of untracked files older than " +
      s"${new Date(deleteBeforeTimestamp).toGMTString} in $path")
    val hadoopConf = spark.sparkContext.broadcast(
      new SerializableConfiguration(sessionHadoopConf))
    val basePath = fs.makeQualified(path).toString


    val snapshot = snapshotManagement.snapshot
    val vPath = new Path(basePath)
    val validFiles = snapshot.allDataInfoDS
      .withColumn("path", transDataFilePathToRelativeUDF(vPath, hadoopConf.value)(col("file_path")))
      .select("path")

    val partitionColumns = snapshot.getTableInfo.range_partition_columns

    val parallelism = spark.conf.get(StarLakeSQLConf.CLEANUP_PARALLELISM)

    val allFilesAndDirs =
      FileOperation.recursiveListDirs(
        spark,
        Seq(basePath),
        hadoopConf,
        hiddenFileNameFilter = StarLakeUtils.isHiddenDirectory(partitionColumns, _),
        fileListingParallelism = Option(parallelism))
    try {
      allFilesAndDirs.cache()

      val dirCounts = allFilesAndDirs.where('isDir).count() + 1 // +1 for the base path

      // The logic below is as follows:
      //   1. We take all the files and directories listed in our reservoir
      //   2. We filter all files older than our tombstone retention period and directories
      //   3. We get the subdirectories of all files so that we can find non-empty directories
      //   4. We groupBy each path, and count to get how many files are in each sub-directory
      //   5. We subtract all the valid files and tombstones in our state
      //   6. We filter all paths with a count of 1, which will correspond to files not in the
      //      state, and empty directories. We can safely delete all of these
      val diff = allFilesAndDirs
        .where('modificationTime < deleteBeforeTimestamp || 'isDir)
        .mapPartitions { fileStatusIterator =>
          val reservoirBase = new Path(basePath)
          val fs = reservoirBase.getFileSystem(hadoopConf.value.value)
          fileStatusIterator.flatMap { fileStatus =>
            if (fileStatus.isDir) {
              Iterator.single(relativize(fileStatus.getPath, fs, reservoirBase, isDir = true))
            } else {
              val dirs = getAllSubdirs(basePath, fileStatus.path, fs)
              val dirsWithSlash = dirs.map { p =>
                relativize(new Path(p), fs, reservoirBase, isDir = true)
              }
              dirsWithSlash ++ Iterator(
                relativize(new Path(fileStatus.path), fs, reservoirBase, isDir = false))
            }
          }
        }.groupBy($"value" as 'path)
        .count()
        .join(validFiles, Seq("path"), "leftanti")
        .where('count === 1)
        .select('path)
        .as[String]
        .map { relativePath =>
          assert(!stringToPath(relativePath).isAbsolute,
            "Shouldn't have any absolute paths for deletion here.")
          pathToString(FileOperation.absolutePath(basePath, relativePath))
        }

      if (dryRun) {
        val numFiles = diff.count()

        logInfo(s"Found $numFiles files and directories in a total of " +
          s"$dirCounts directories that are safe to delete.")

        return diff.map(f => stringToPath(f).toString).toDF("path")
      }
      logInfo(s"Deleting untracked files and empty directories in $path")

      val canConcurrentDelete = spark.conf.get(StarLakeSQLConf.CLEANUP_CONCURRENT_DELETE_ENABLE)
      val filesDeleted = if(canConcurrentDelete){
        deleteConcurrently(
          spark,
          diff,
          snapshotManagement.table_name,
          spark.sparkContext.broadcast(new SerializableConfiguration(sessionHadoopConf)))
      }else{
        delete(diff, fs)
      }


      logInfo(s"Deleted $filesDeleted files and directories in a total " +
        s"of $dirCounts directories.")

      spark.createDataset(Seq(basePath)).toDF("path")
    } finally {
      allFilesAndDirs.unpersist()
    }
  }


}

trait CleanupCommandImpl extends Logging {

  protected def transDataFilePathToRelative(file: String, basePath: Path, fs: FileSystem): String = {
    val filePath = new Path(file)
    pathToString(FileOperation.tryRelativizePath(fs, basePath, filePath))
  }

  protected def transDataFilePathToRelativeUDF(basePath: Path,
                                               hadoopConf: SerializableConfiguration): UserDefinedFunction =
    udf((file: String) => {
      val fs = basePath.getFileSystem(hadoopConf.value)
      transDataFilePathToRelative(file, basePath, fs)
    })

  /**
    * Attempts to relativize the `path` with respect to the `reservoirBase` and converts the path to
    * a string.
    */
  protected def relativize(path: Path,
                           fs: FileSystem,
                           reservoirBase: Path,
                           isDir: Boolean): String = {
    pathToString(FileOperation.tryRelativizePath(fs, reservoirBase, path))
  }

  /**
    * Wrapper function for FileOperations.getAllSubDirectories
    * returns all subdirectories that `file` has with respect to `base`.
    */
  protected def getAllSubdirs(base: String, file: String, fs: FileSystem): Iterator[String] = {
    FileOperation.getAllSubDirectories(base, file)._1
  }

  /**
    * Attempts to delete the list of candidate files. Returns the number of files deleted.
    */
  protected def delete(diff: Dataset[String], fs: FileSystem): Long = {
    val fileResultSet = diff.toLocalIterator().asScala
    fileResultSet.map(p => stringToPath(p)).count(f => FileOperation.tryDeleteNonRecursive(fs, f))
  }
  protected def deleteConcurrently(spark: SparkSession,
                       diff: Dataset[String],
                       tablePath: String,
                       hadoopConf: Broadcast[SerializableConfiguration]): Long = {
    import spark.implicits._
    diff.mapPartitions { files =>
      val fs = new Path(tablePath).getFileSystem(hadoopConf.value.value)
      val filesDeletedPerPartition =
        files.map(p => stringToPath(p)).count(f => FileOperation.tryDeleteNonRecursive(fs, f))
      Iterator(filesDeletedPerPartition)
    }.reduce(_ + _)
  }

  protected def stringToPath(path: String): Path = new Path(new URI(path))

  protected def pathToString(path: Path): String = path.toUri.toString


}
