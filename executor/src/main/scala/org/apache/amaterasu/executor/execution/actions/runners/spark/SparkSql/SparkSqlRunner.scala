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
import org.apache.amaterasu.executor.runtime.AmaContext
import org.apache.commons.io.FilenameUtils
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
  * Created by kirupa on 11/12/16.
  * Amaterasu currently supports JSON and PARQUET as data sources.
  * CSV data source support will be provided in later versions.
  */
class SparkSqlRunner extends Logging {
  var env: Environment = _
  var notifier: Notifier = _
  var jobId: String = _
  var actionName: String = _
  var spark: SparkSession = _

  /*
  Method: executeQuery
  Description: when user specifies query in amaterasu format, this method parse and executes the query.
               If not in Amaterasu format, then directly executes the query
  @Params: query string
   */
  def executeQuery(query: String): Unit = {

    notifier.info(s"================= executing the SQL query =================")
    if (!query.isEmpty) {

      if (query.toLowerCase.contains("amacontext")) {

        //Parse the incoming query
        notifier.info(s"================= parsing the SQL query =================")

        val parser: List[String] = query.toLowerCase.split(" ").toList
        var sqlPart: String = ""

        //get only the sql part of the query
        for (i <- 0 to parser.indexOf("from")) {
          sqlPart += parser(i) + " "
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
        val parsedQuery = sqlPart + locationPath

        //Load the dataframe from previous action
        val loadData: DataFrame = AmaContext.getDataFrame(actionName, dfName, fileFormat)
        loadData.createOrReplaceTempView(locationPath)

        notifier.info(s"================= executing the SQL query =================")

        val sqlDf = spark.sql(parsedQuery)
        //@TODO: outputFileFormat should be read from YAML file instead of input fileformat
        writeDf(sqlDf, fileFormat, env.workingDir, jobId, actionName)

        notifier.info(s"================= finished action $actionName =================")
      }
      else {

        notifier.info(s"================= executing the SQL query =================")

        val fildDf = spark.sql(query)
        //@TODO: outputFileFormat should be read from YAML file instead of output fileFormat being empty
        writeDf(fildDf, "", env.workingDir, jobId, actionName)

        notifier.info(s"================= finished action $actionName =================")
      }
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

  /*
  Method to write dataframes to a specified format
  @Params
  df: Dataframe to be written
  fileFormat: same as input file format
  workingDir: temp directory
  jobId, actionName: As specified by the user
  */
  def writeDf(df: DataFrame, outputFileFormat: String, workingDir: String, jobId: String, actionName: String): Unit = {
    outputFileFormat.toLowerCase match {
      case "parquet" => df.write.mode(SaveMode.Overwrite).parquet(s"$workingDir/$jobId/$actionName/" + actionName + "Df")
      case "json" => df.write.mode(SaveMode.Overwrite).json(s"$workingDir/$jobId/$actionName/" + actionName + "Df")
      case "csv" => df.write.mode(SaveMode.Overwrite).csv(s"$workingDir/$jobId/$actionName/" + actionName + "Df")
      case "orc" => df.write.mode(SaveMode.Overwrite).orc(s"$workingDir/$jobId/$actionName/" + actionName + "Df")
      case "text" => df.write.mode(SaveMode.Overwrite).text(s"$workingDir/$jobId/$actionName/" + actionName + "Df")
      //case "jdbc" => df.write.mode(SaveMode.Overwrite).jdbc(s"$workingDir/$jobId/$actionName/" + actionName + "Df")
      case _ => df.write.mode(SaveMode.Overwrite).parquet(s"$workingDir/$jobId/$actionName/" + actionName + "Df")
    }
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
