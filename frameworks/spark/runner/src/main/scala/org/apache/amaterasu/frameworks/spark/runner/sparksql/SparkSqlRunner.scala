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
package org.apache.amaterasu.frameworks.spark.runner.sparksql

import java.io.File
import java.util

import org.apache.amaterasu.common.execution.actions.Notifier
import org.apache.amaterasu.common.logging.Logging
import org.apache.amaterasu.common.runtime.Environment
import org.apache.amaterasu.frameworks.spark.runtime.AmaContext
import org.apache.amaterasu.sdk.AmaterasuRunner
import org.apache.commons.io.FilenameUtils
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

import scala.collection.JavaConverters._

/**
  * Amaterasu currently supports JSON and PARQUET as data sources.
  * CSV data source support will be provided in later versions.
  */
class SparkSqlRunner extends Logging with AmaterasuRunner {
  var env: Environment = _
  var notifier: Notifier = _
  var jobId: String = _
  //var actionName: String = _
  var spark: SparkSession = _

  /*
  Method: executeQuery
  Description: when user specifies query in amaterasu format, this method parse and executes the query.
               If not in Amaterasu format, then directly executes the query
  @Params: query string
   */
  override def executeSource(actionSource: String, actionName: String, exports: util.Map[String, String]): Unit = {

    notifier.info(s"================= Started action $actionName =================")

    if (!actionSource.isEmpty) {

      var result: DataFrame = null
      if (actionSource.toLowerCase.contains("amacontext")) {

        //Parse the incoming query
        //notifier.info(s"================= parsing the SQL query =================")

        val parser: List[String] = actionSource.toLowerCase.split(" ").toList
        var sqlPart1: String = ""
        var sqlPart2: String = ""
        var queryTempLen: Int = 0

        //get only the sql part of the query
        for (i <- 0 to parser.indexOf("from")) {
          sqlPart1 += parser(i) + " "
        }

        if (parser.indexOf("readas") == -1) {
          queryTempLen = parser.length - 1
        }
        else
          queryTempLen = parser.length - 3

        for (i <- parser.indexOf("from") + 1 to queryTempLen) {
          if (!parser(i).contains("amacontext"))
            sqlPart2 += " " + parser(i)
        }

        //If no read format is speicified by the user, use PARQUET as default file format to load data
        var fileFormat: String = null
        //if there is no index for "readas" keyword, then set PARQUET as default read format
        if (parser.indexOf("readas") == -1) {
          fileFormat = "parquet"
        }
        else
          fileFormat = parser(parser.indexOf("readas") + 1)


        val locationPath: String = parser.filter(word => word.contains("amacontext")).mkString("")
        val directories = locationPath.split("_")
        val actionName = directories(1)
        val dfName = directories(2)
        val parsedQuery = sqlPart1 + locationPath + sqlPart2

        //Load the dataframe from previous action
        val loadData: DataFrame = AmaContext.getDataFrame(actionName, dfName, fileFormat)
        loadData.createOrReplaceTempView(locationPath)


        try{

          result = spark.sql(parsedQuery)
          notifier.success(parsedQuery)
        } catch {
          case e: Exception => notifier.error(parsedQuery, e.getMessage)
        }

      }
      else {

        notifier.info("Executing SparkSql on: " + actionSource)

        result = spark.sql(actionSource)
      }
      val exportsBuff = exports.asScala.toBuffer
      if (exportsBuff.nonEmpty) {
        val exportName = exportsBuff.head._1
        val exportFormat = exportsBuff.head._2
        //notifier.info(s"exporting to -> ${env.workingDir}/$jobId/$actionName/$exportName")
        result.write.mode(SaveMode.Overwrite).format(exportFormat).save(s"${env.workingDir}/$jobId/$actionName/$exportName")
      }
      notifier.info(s"================= finished action $actionName =================")
    }
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

  override def getIdentifier: String = "sql"

}

object SparkSqlRunner {

  def apply(env: Environment,
            jobId: String,
            // actionName: String,
            notifier: Notifier,
            spark: SparkSession): SparkSqlRunner = {

    val sparkSqlRunnerObj = new SparkSqlRunner

    sparkSqlRunnerObj.env = env
    sparkSqlRunnerObj.jobId = jobId
    //sparkSqlRunnerObj.actionName = actionName
    sparkSqlRunnerObj.notifier = notifier
    sparkSqlRunnerObj.spark = spark
    sparkSqlRunnerObj
  }
}
