package io.shinto.amaterasu.execution.actions.runners.spark

import io.shinto.amaterasu.Logging
import io.shinto.amaterasu.execution.actions.Notifier
import io.shinto.amaterasu.runtime.{ AmaContext, Environment }
import org.apache.spark.SparkContext
import org.apache.spark.sql.{ SQLContext, SaveMode }

/**
  * Created by kirupa on 11/12/16.
  */
class SparkSqlRunner extends Logging {
  var env: Environment = null
  var notifier: Notifier = null
  var jobId: String = null
  var actionName: String = null
  var sc: SparkContext = null

  def executeQuery(
    sparkSqlTempTable: String,
    dataSource: String,
    query: String
  ) = {

    notifier.info(s"================= started action $actionName =================")
    val sqlContext = AmaContext.sqlContext
    val loadData = sqlContext.read.json(dataSource)
    loadData.registerTempTable(sparkSqlTempTable)

    if (!query.isEmpty) {
      val sqlDf = sqlContext.sql(query)
      sqlDf.write.mode(SaveMode.Overwrite).parquet(s"${env.workingDir}/$jobId/$actionName")
    }

    AmaContext.sc.stop()
    notifier.info(s"================= finished action $actionName =================")
  }

}

object SparkSqlRunner {

  def apply(
    env: Environment,
    jobId: String,
    actionName: String,
    notifier: Notifier,
    sc: SparkContext
  ): SparkSqlRunner = {
    var started: Boolean = false
    if (!started) {
      AmaContext.init(sc, new SQLContext(sc), jobId, env)
      started = true
    }
    val sparkSqlRunnerObj = new SparkSqlRunner

    sparkSqlRunnerObj.env = env
    sparkSqlRunnerObj.jobId = jobId
    sparkSqlRunnerObj.actionName = actionName
    sparkSqlRunnerObj.notifier = notifier
    sparkSqlRunnerObj.sc = sc

    sparkSqlRunnerObj
  }
}
