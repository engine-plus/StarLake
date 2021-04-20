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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs._
import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging

import scala.util.control.NonFatal

/**
  * The [[LogStore]] implementation for HDFS, which uses Hadoop [[FileContext]] API's to
  * provide the necessary atomic and durability guarantees:
  *
  * 1. Atomic visibility of files: `FileContext.rename` is used write files which is atomic for HDFS.
  *
  * 2. Consistent file listing: HDFS file listing is consistent.
  */
class HDFSLogStore(sparkConf: SparkConf, defaultHadoopConf: Configuration)
  extends HadoopFileSystemLogStore(sparkConf, defaultHadoopConf) with Logging {

  protected def getFileContext(path: Path): FileContext = {
    FileContext.getFileContext(path.toUri, getHadoopConfiguration)
  }

  val noAbstractFileSystemExceptionMessage = "No AbstractFileSystem"

  def write(path: Path, actions: Iterator[String], overwrite: Boolean = false): Unit = {
    throw new Exception("We won't use this to write hdfs file.")
  }

  private def tryRemoveCrcFile(fc: FileContext, path: Path): Unit = {
    try {
      val checksumFile = new Path(path.getParent, s".${path.getName}.crc")
      if (fc.util.exists(checksumFile)) {
        // checksum file exists, deleting it
        fc.delete(checksumFile, true)
      }
    } catch {
      case NonFatal(_) => // ignore, we are removing crc file as "best-effort"
    }
  }

  override def isPartialWriteVisible(path: Path): Boolean = true
}
