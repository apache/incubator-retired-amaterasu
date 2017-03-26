package io.shinto.amaterasu.executor.runtime

import io.shinto.amaterasu.common.logging.Logging
import io.shinto.amaterasu.common.runtime.Environment
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext, SaveMode}

import scala.reflect.ClassTag

object AmaContext extends Logging {

  var sc: SparkContext = _
  var jobId: String = _
  var sqlContext: SQLContext = _
  var env: Environment = _

  def init(sc: SparkContext,
           sqlContext: SQLContext,
           jobId: String,
           env: Environment) = {

    AmaContext.sc = sc
    AmaContext.sqlContext = sqlContext
    AmaContext.jobId = jobId
    AmaContext.env = env

  }

  def saveDataFrame(df: DataFrame, actionName: String, dfName: String) = {

    try {

      log.debug(s"${env.workingDir}/$jobId/$actionName/$dfName")
      df.write.mode(SaveMode.Overwrite).parquet(s"${env.workingDir}/$jobId/$actionName/$dfName")

    }
    catch {
      case e: Exception => {
        log.error(s"failed storing DataFrame: ${e.getMessage}")
      }

    }
  }

  def saveRDD(rdd: RDD[_], actionName: String, rddName: String) = {

    try {

      log.debug(s"${env.workingDir}/$jobId/$actionName/$rddName")
      rdd.saveAsObjectFile(s"${env.workingDir}/$jobId/$actionName/$rddName")

    }
    catch {
      case e: Exception => {
        log.error(s"failed storing RDD: ${e.getMessage}")
      }

    }

  }

  def getDataFrame(actionName: String, dfName: String): DataFrame = {

    sqlContext.read.parquet(s"${env.workingDir}/$jobId/$actionName/$dfName")

  }

  def getRDD[T: ClassTag](actionName: String, rddName: String): RDD[T] = {

    sc.objectFile[T](s"${env.workingDir}/$jobId/$actionName/$rddName")

  }

  def getActionResult(actionName: String): DataFrame = {

    sqlContext.sql(s"select * from ${AmaContext.jobId}.$actionName")

  }

}
