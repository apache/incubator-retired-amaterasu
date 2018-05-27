package org.apache.amaterasu.executor.runner.spark

import org.apache.amaterasu.common.logging.Logging
import org.apache.amaterasu.common.runtime.Environment
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Dataset, Encoder, SparkSession}

/**
  * @author Arun Manivannan
  */
object AmaContext extends Logging {

  var spark: SparkSession = _
  var sc: SparkContext = _
  var jobId: String = _
  var env: Environment = _

  def init(spark: SparkSession,
           jobId: String,
           env: Environment): Unit = {

    AmaContext.spark = spark
    AmaContext.sc = spark.sparkContext
    AmaContext.jobId = jobId
    AmaContext.env = env

  }

  def getDataFrame(actionName: String, dfName: String, format: String = "parquet"): DataFrame = {
    spark.read.format(format).load(s"${env.workingDir}/$jobId/$actionName/$dfName")
  }

  def getDataset[T: Encoder](actionName: String, dfName: String, format: String = "parquet"): Dataset[T] = {
    getDataFrame(actionName, dfName, format).as[T]
  }

}
