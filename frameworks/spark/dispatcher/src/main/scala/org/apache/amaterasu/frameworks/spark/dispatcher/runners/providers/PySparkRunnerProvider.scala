package org.apache.amaterasu.frameworks.spark.dispatcher.runners.providers
import org.apache.amaterasu.common.configuration.ClusterConfig
import org.apache.amaterasu.common.dataobjects.ActionData
import org.apache.amaterasu.frameworks.python.dispatcher.runners.providers.PythonRunnerProviderBase

class PySparkRunnerProvider(val env: String, val conf: ClusterConfig) extends PythonRunnerProviderBase(env, conf) {

  override def getCommand(jobId: String, actionData: ActionData, env: String, executorId: String, callbackAddress: String): String = {
    var command = super.getCommand(jobId: String, actionData: ActionData, env: String, executorId: String, callbackAddress: String)
    log.info(s"===> Cluster manager: ${conf.mode}")
    command = command + conf.mode match {
      case "mesos" =>
          s" && env AMA_NODE=${sys.env("AMA_NODE")} env MESOS_NATIVE_JAVA_LIBRARY=${conf.mesos.libPath}" +
          s" && python3 ${actionData.getSrc}"
      case "yarn" => s" && /bin/bash spark/bin/load-spark-env.sh" +
                     s" && python3 ${actionData.getSrc}"
      case _ => ""
    }
    command
  }

  override def getRunnerResources: Array[String] = {
    val resources = super.getRunnerResources
    resources :+ s"amaterasu_pyspark-${conf.version}.zip"
    resources
  }


  override def getHasExecutor: Boolean = true

  override def getActionUserResources(jobId: String, actionData: ActionData): Array[String] = Array.empty[String]
}

object PySparkRunnerProvider {
  def apply(env: String, conf: ClusterConfig): PySparkRunnerProvider = {
    val result = new PySparkRunnerProvider(env, conf)
    result
  }
}