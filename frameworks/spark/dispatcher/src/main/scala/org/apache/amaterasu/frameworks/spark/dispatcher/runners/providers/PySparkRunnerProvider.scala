package org.apache.amaterasu.frameworks.spark.dispatcher.runners.providers

import org.apache.amaterasu.common.configuration.ClusterConfig
import org.apache.amaterasu.common.dataobjects.ActionData
import org.apache.amaterasu.frameworks.python.dispatcher.runners.providers.PythonRunnerProviderBase

class PySparkRunnerProvider(val env: String, val conf: ClusterConfig) extends PythonRunnerProviderBase(env, conf) {

  override def getCommand(jobId: String, actionData: ActionData, env: String, executorId: String, callbackAddress: String): String = {
    val command = super.getCommand(jobId: String, actionData: ActionData, env: String, executorId: String, callbackAddress: String)
    log.info(s"===> Cluster manager: ${conf.mode}")
    command +
      //s" $$SPARK_HOME/conf/spark-env.sh" +
     // s" && env PYSPARK_PYTHON=$getVirtualPythonPath" +
      //s" env PYSPARK_DRIVER_PYTHON=$getVirtualPythonPath" + d
        s" && $$SPARK_HOME/bin/spark-submit --master yarn-cluster --conf spark.pyspark.python=$getVirtualPythonPath --files $$SPARK_HOME/conf/hive-site.xml ${actionData.getSrc}"
  }

  override def getRunnerResources: Array[String] = {
    var resources = super.getRunnerResources
    resources = resources :+ s"amaterasu_pyspark-${conf.version}.zip"
    log.info(s"PYSPARK RESOURCES ==> ${resources.toSet}")
    resources
  }


  override def getHasExecutor: Boolean = false

  override def getActionUserResources(jobId: String, actionData: ActionData): Array[String] = Array[String]()
}

object PySparkRunnerProvider {
  def apply(env: String, conf: ClusterConfig): PySparkRunnerProvider = {
    val result = new PySparkRunnerProvider(env, conf)
    result
  }
}