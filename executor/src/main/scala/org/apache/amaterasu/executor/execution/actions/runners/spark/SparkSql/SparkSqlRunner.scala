/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.amaterasu.executor.execution.actions.runners.spark.SparkSql

import java.io.File
import org.apache.amaterasu.common.execution.actions.Notifier
import org.apache.amaterasu.common.logging.Logging
import org.apache.amaterasu.common.runtime.Environment
import org.apache.commons.io.FilenameUtils
import org.apache.spark.sql.{SparkSession, SaveMode,DataFrame}

/**
  * Amaterasu currently supports JSON and PARQUET as data sources.
  * CSV data source support will be provided in the later versions.
  */
class SparkSqlRunner extends Logging {
  var env: Environment = _
  var notifier: Notifier = _
  var jobId: String = _
  var actionName: String = _
  var spark: SparkSession = _

  def executeQuery(sparkSqlTempTable: String,
                   dataSource: String,
                   query: String) = {

    notifier.info(s"================= started action $actionName =================")
    val file: File = new File(dataSource)
    notifier.info(s"================= auto-detecting file type of data source =================")
    val loadData: DataFrame = file match {
      case _ if file.isFile => FilenameUtils.getExtension(file.toString) match {
        case "json" => spark.read.json(dataSource)
        case "parquet" => spark.read.parquet(dataSource)
      }
      case _ if file.isDirectory => {
        val extensions = findFileType(file)
        extensions match {
          case _ if extensions.contains("json") => spark.read.json(dataSource)
          case _ if extensions.contains("parquet") => spark.read.parquet(dataSource)
        }
      }
    }

    loadData.createOrReplaceTempView(sparkSqlTempTable)
    notifier.info(s"================= executing the SQL query =================")
    if (!query.isEmpty) {
      val sqlDf = spark.sql(query)
      println(s"${env.workingDir}/$jobId/$actionName")
      sqlDf.write.mode(SaveMode.Overwrite).parquet(s"${env.workingDir}/$jobId/$actionName")
    }

    notifier.info(s"================= finished action $actionName =================")
  }

  /*
  Method to find the file type of files within a directory
  @Params
  folderName : Path to location of the directory containing data-source files
  */

  def findFileType(folderName: File): Array[String] = {
    // get all the files from a directory
    val files: Array[File] = folderName.listFiles()
    val extensions: Array[String] = files.map(file => FilenameUtils.getExtension(file.toString))
    extensions
  }

}

object SparkSqlRunner {

  def apply(env: Environment,
            jobId: String,
            actionName: String,
            notifier: Notifier,
            spark: SparkSession): SparkSqlRunner = {

    val sparkSqlRunnerObj = new SparkSqlRunner

    sparkSqlRunnerObj.env = env
    sparkSqlRunnerObj.jobId = jobId
    sparkSqlRunnerObj.actionName = actionName
    sparkSqlRunnerObj.notifier = notifier
    sparkSqlRunnerObj.spark = spark
    sparkSqlRunnerObj
  }
}
