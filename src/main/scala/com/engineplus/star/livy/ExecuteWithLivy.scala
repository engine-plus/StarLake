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

package com.engineplus.star.livy

import java.io.FileNotFoundException
import java.net.URI

import com.engineplus.star.tables.StarTable
import org.apache.livy.{Job, JobContext, LivyClient, LivyClientBuilder}


object ExecuteWithLivy {

  def getLivyClient(conf: Map[String, String]): LivyClient = {
    val livyClientBuilder = new LivyClientBuilder()
      .setURI(new URI(conf.getOrElse("spark.livy.host", "http://localhost:8998")))

    conf.foreach(m => livyClientBuilder.setConf(m._1, m._2))
    livyClientBuilder.build()
  }

  def getSourcePath(obj: Object): String = {
    val source = obj.getClass.getProtectionDomain.getCodeSource
    if (source != null && source.getLocation.getPath != "") {
      source.getLocation.getPath
    } else {
      throw new FileNotFoundException(s"Jar containing ${obj.getClass.getName} not found.")
    }
  }
}

class CompactionJob(tableName: String, force: Boolean)
  extends Job[Unit] {
  override def call(jobContext: JobContext): Unit = {
    val ss = jobContext.sqlctx().sparkSession
    StarTable.forPath(ss, tableName).compaction(force)
  }
}

class CompactionJobWithCondition(tableName: String, condition: String, force: Boolean)
  extends Job[Unit] {
  override def call(jobContext: JobContext): Unit = {
    val ss = jobContext.sqlctx().sparkSession
    StarTable.forPath(ss, tableName).compaction(condition, force)
  }
}
