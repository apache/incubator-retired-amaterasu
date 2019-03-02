package org.apache.amaterasu.frameworks.spark.dispatcher.runners.providers

import java.net.URLEncoder

import org.apache.amaterasu.common.configuration.ClusterConfig
import org.apache.amaterasu.common.dataobjects.ActionData
import org.apache.amaterasu.frameworks.python.dispatcher.runners.providers.PythonRunnerProviderBase
import org.apache.amaterasu.leader.common.utilities.DataLoader
import org.apache.amaterasu.sdk.frameworks.RunnerSetupProvider
import org.apache.hadoop.yarn.api.ApplicationConstants

class PySparkRunnerProvider(val env: String, val conf: ClusterConfig) extends PythonRunnerProviderBase(env, conf) {

  private val libPath = System.getProperty("java.library.path")

  override def getCommand(jobId: String, actionData: ActionData, env: String, executorId: String, callbackAddress: String): String = {
    val command = super.getCommand(jobId: String, actionData: ActionData, env: String, executorId: String, callbackAddress: String)
    command + conf.mode match {
      case "mesos" =>
          s" && env AMA_NODE=${sys.env("AMA_NODE")} env MESOS_NATIVE_JAVA_LIBRARY=${conf.mesos.libPath}" +
          s" && python3 ${actionData.getSrc}"
      case "yarn" => s" && /bin/bash spark/bin/load-spark-env.sh" +
                     s" && python3 ${actionData.getSrc}"
      case _ => ""
    }
  }

  override def getRunnerResources: Array[String] = {
    val resources = super.getRunnerResources
    resources :+ "amaterasu_pyspark.zip"
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