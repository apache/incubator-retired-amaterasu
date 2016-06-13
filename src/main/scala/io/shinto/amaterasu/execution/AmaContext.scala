package io.shinto.amaterasu.execution

import io.shinto.amaterasu.configuration.environments.Environment
import org.apache.spark.sql.{ DataFrame, SQLContext }
import org.apache.spark.SparkContext

object AmaContext {

  var sc: SparkContext = null
  var jobId: String = null
  var sqlContext: SQLContext = null
  var env: Environment = null

  def init(sc: SparkContext,
           sqlContext: SQLContext,
           jobId: String,
           env: Environment) = {

    AmaContext.sc = sc
    AmaContext.sqlContext = sqlContext
    AmaContext.jobId = jobId
    AmaContext.env = env

  }

  def persistDataFrame(df: DataFrame,
                       actionName: String,
                       dfName: String) = {

    df.write.parquet(s"${env.workingDir}/$jobId/$actionName/$dfName")

  }

  def getDataFrame(actionName: String,
                   dfName: String): DataFrame = {

    AmaContext.sqlContext.read.parquet(s"${env.workingDir}/$jobId/$actionName/$dfName")

  }

  def getActionResult(actionName: String): DataFrame = {

    AmaContext.sqlContext.sql(s"select * from ${AmaContext.jobId}.$actionName")

  }

}
