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

package org.apache.spark.sql.star.storage

import java.io.FileNotFoundException
import java.net.URI
import java.util.concurrent.{ConcurrentHashMap, TimeUnit}

import com.google.common.cache.CacheBuilder
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.spark.SparkConf

import scala.collection.JavaConverters._

/**
  * Single Spark-driver/JVM LogStore implementation for S3.
  *
  * We assume the following from S3's [[FileSystem]] implementations:
  * - File writing on S3 is all-or-nothing, whether overwrite or not.
  * - List-after-write can be inconsistent.
  *
  * Regarding file creation, this implementation:
  * - Opens a stream to write to S3 (regardless of the overwrite option).
  * - Failures during stream write may leak resources, but may never result in partial writes.
  *
  * Regarding directory listing, this implementation:
  * - returns a list by merging the files listed from S3 and recently-written files from the cache.
  */
class S3SingleDriverLogStore(
                              sparkConf: SparkConf,
                              hadoopConf: Configuration) extends HadoopFileSystemLogStore(sparkConf, hadoopConf) {

  import S3SingleDriverLogStore._

  private def resolved(path: Path): (FileSystem, Path) = {
    val fs = path.getFileSystem(getHadoopConfiguration)
    val resolvedPath = stripUserInfo(fs.makeQualified(path))
    (fs, resolvedPath)
  }

  private def getPathKey(resolvedPath: Path): Path = {
    stripUserInfo(resolvedPath)
  }

  private def stripUserInfo(path: Path): Path = {
    val uri = path.toUri
    val newUri = new URI(
      uri.getScheme,
      null,
      uri.getHost,
      uri.getPort,
      uri.getPath,
      uri.getQuery,
      uri.getFragment)
    new Path(newUri)
  }

  /**
    * Merge two iterators of [[FileStatus]] into a single iterator ordered by file path name.
    * In case both iterators have [[FileStatus]]s for the same file path, keep the one from
    * `iterWithPrecedence` and discard that from `iter`.
    */
  private def mergeFileIterators(
                                  iter: Iterator[FileStatus],
                                  iterWithPrecedence: Iterator[FileStatus]): Iterator[FileStatus] = {
    (iter.map(f => (f.getPath, f)).toMap ++ iterWithPrecedence.map(f => (f.getPath, f)))
      .values
      .toSeq
      .sortBy(_.getPath.getName)
      .iterator
  }

  /**
    * List files starting from `resolvedPath` (inclusive) in the same directory.
    */
  private def listFromCache(fs: FileSystem, resolvedPath: Path) = {
    val pathKey = getPathKey(resolvedPath)
    writtenPathCache
      .asMap()
      .asScala
      .iterator
      .filter { case (path, _) =>
        path.getParent == pathKey.getParent() && path.getName >= pathKey.getName
      }
      .map { case (path, fileMetadata) =>
        new FileStatus(
          fileMetadata.length,
          false,
          1,
          fs.getDefaultBlockSize(path),
          fileMetadata.modificationTime,
          path)
      }
  }

  /**
    * List files starting from `resolvedPath` (inclusive) in the same directory, which merges
    * the file system list and the cache list when `useCache` is on, otherwise
    * use file system list only.
    */
  private def listFromInternal(fs: FileSystem, resolvedPath: Path, useCache: Boolean = true) = {
    val parentPath = resolvedPath.getParent
    if (!fs.exists(parentPath)) {
      throw new FileNotFoundException(s"No such file or directory: $parentPath")
    }
    val listedFromFs =
      fs.listStatus(parentPath).filter(_.getPath.getName >= resolvedPath.getName).iterator
    val listedFromCache = if (useCache) listFromCache(fs, resolvedPath) else Iterator.empty

    // File statuses listed from file system take precedence
    mergeFileIterators(listedFromCache, listedFromFs)
  }

  /**
    * List files starting from `resolvedPath` (inclusive) in the same directory.
    */
  override def listFrom(path: Path): Iterator[FileStatus] = {
    val (fs, resolvedPath) = resolved(path)
    listFromInternal(fs, resolvedPath)
  }


  override def write(path: Path, actions: Iterator[String], overwrite: Boolean = false): Unit = {

    throw new Exception("We won't use this to write s3 file.")
  }

  override def isPartialWriteVisible(path: Path): Boolean = false

  override def invalidateCache(): Unit = {
    writtenPathCache.invalidateAll()
  }
}

object S3SingleDriverLogStore {
  /**
    * A global path lock to ensure that no concurrent writers writing to the same path in the same
    * JVM.
    */
  private val pathLock = new ConcurrentHashMap[Path, AnyRef]()

  /**
    * A global cache that records the metadata of the files recently written.
    * As list-after-write may be inconsistent on S3, we can use the files in the cache
    * to fix the inconsistent file listing.
    */
  private val writtenPathCache =
    CacheBuilder.newBuilder()
      .expireAfterAccess(120, TimeUnit.MINUTES)
      .build[Path, FileMetadata]()

  /**
    * Release the lock for the path after writing.
    *
    * Note: the caller should resolve the path to make sure we are locking the correct absolute path.
    */
  private def releasePathLock(resolvedPath: Path): Unit = {
    val lock = pathLock.remove(resolvedPath)
    lock.synchronized {
      lock.notifyAll()
    }
  }

  /**
    * Acquire a lock for the path before writing.
    *
    * Note: the caller should resolve the path to make sure we are locking the correct absolute path.
    */
  private def acquirePathLock(resolvedPath: Path): Unit = {
    while (true) {
      val lock = pathLock.putIfAbsent(resolvedPath, new Object)
      if (lock == null) return
      lock.synchronized {
        while (pathLock.get(resolvedPath) == lock) {
          lock.wait()
        }
      }
    }
  }
}

/**
  * The file metadata to be stored in the cache.
  */
case class FileMetadata(length: Long, modificationTime: Long)
