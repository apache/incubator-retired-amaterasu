package io.shinto.amaterasu.common.execution.actions.runners.spark

import java.io.File

import io.shinto.amaterasu.common.execution.actions.Notifier
import io.shinto.amaterasu.common.logging.Logging
import io.shinto.amaterasu.common.runtime.Environment
import io.shinto.amaterasu.executor.runtime.AmaContext
import org.apache.commons.io.FilenameUtils
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

/**
  * Created by kirupa on 11/12/16.
  * Amaterasu currently supports JSON and PARQUET as data sources.
  * CSV data source support will be provided in the later versions.
  */
class SparkSqlRunner extends Logging {
  var env: Environment = null
  var notifier: Notifier = null
  var jobId: String = null
  var actionName: String = null
  var sc: SparkContext = null

  def executeQuery(sparkSqlTempTable: String,
                    dataSource: String,
                    query: String) = {

    notifier.info(s"================= started action $actionName =================")
    val sqlContext = AmaContext.sqlContext
    val file: File = new File(dataSource)
    notifier.info(s"================= auto-detecting file type of data source =================")
    val loadData: DataFrame = file match {
      case _ if (file.isFile) => FilenameUtils.getExtension(file.toString) match {
        case "json" => sqlContext.read.json(dataSource)
        case "parquet" => sqlContext.read.parquet(dataSource)
      }
      case _ if (file.isDirectory) => {
        val extensions = findFileType(file)
        extensions match {
          case _ if (extensions.contains("json")) => sqlContext.read.json(dataSource)
          case _ if (extensions.contains("parquet")) => sqlContext.read.parquet(dataSource)
        }
      }
    }

    loadData.registerTempTable(sparkSqlTempTable)
    notifier.info(s"================= executing the SQL query =================")
    if (!query.isEmpty) {
      val sqlDf = sqlContext.sql(query)
      println(s"${env.workingDir}/$jobId/$actionName")
      sqlDf.write.mode(SaveMode.Overwrite).parquet(s"${env.workingDir}/$jobId/$actionName")
    }

    //AmaContext.sc.stop()
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
            sc: SparkContext): SparkSqlRunner = {

    val sparkSqlRunnerObj = new SparkSqlRunner

    sparkSqlRunnerObj.env = env
    sparkSqlRunnerObj.jobId = jobId
    sparkSqlRunnerObj.actionName = actionName
    sparkSqlRunnerObj.notifier = notifier
    sparkSqlRunnerObj.sc = sc

    sparkSqlRunnerObj
  }
}
