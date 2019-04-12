package org.apache.amaterasu.frameworks.spark.dispatcher.runners.providers
import org.apache.amaterasu.common.configuration.ClusterConfig
import org.apache.amaterasu.common.dataobjects.ActionData
import org.apache.amaterasu.frameworks.python.dispatcher.runners.providers.PythonRunnerProviderBase
import sys.process._

class PySparkRunnerProvider(val env: String, val conf: ClusterConfig) extends PythonRunnerProviderBase(env, conf) {

  override def getCommand(jobId: String, actionData: ActionData, env: String, executorId: String, callbackAddress: String): String = {
    var command = super.getCommand(jobId: String, actionData: ActionData, env: String, executorId: String, callbackAddress: String)
    log.info(s"===> Cluster manager: ${conf.mode}")
    val pythonBinPath = Seq(conf.pythonPath, "-c", "import sys; print(sys.executable)").!!.trim()
    command + s" && /bin/bash $$SPARK_HOME/bin/load-spark-env.sh && env PYSPARK_PYTHON=$pythonBinPath " +
      s" && $$SPARK_HOME/bin/spark-submit ${actionData.getSrc}"
  }

  override def getRunnerResources: Array[String] = {
    var resources = super.getRunnerResources
    resources = resources :+ s"amaterasu_pyspark-${conf.version}.zip"
    log.info(s"PYSPARK RESOURCES ==> ${resources.toSet}")
    resources
  }


  override def getHasExecutor: Boolean = false
}

object PySparkRunnerProvider {
  def apply(env: String, conf: ClusterConfig): PySparkRunnerProvider = {
    val result = new PySparkRunnerProvider(env, conf)
    result
  }
}