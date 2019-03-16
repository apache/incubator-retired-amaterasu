package org.apache.amaterasu.frameworks.spark.dispatcher.runners.providers
import org.apache.amaterasu.common.configuration.ClusterConfig
import org.apache.amaterasu.common.dataobjects.ActionData
import org.apache.amaterasu.frameworks.python.dispatcher.runners.providers.PythonRunnerProviderBase
import sys.process._

class PySparkRunnerProvider(val env: String, val conf: ClusterConfig) extends PythonRunnerProviderBase(env, conf) {

  override def getCommand(jobId: String, actionData: ActionData, env: String, executorId: String, callbackAddress: String): String = {
    var command = super.getCommand(jobId: String, actionData: ActionData, env: String, executorId: String, callbackAddress: String)
    log.info(s"===> Cluster manager: ${conf.mode}")
    val pyhtonBinPath = Seq("python3", "-c", "import sys; print(sys.executable)").!!.trim()
    conf.mode match {
      case "mesos" =>
          command + s" && env PYSPARK_PYTHON=$pyhtonBinPath && env AMA_NODE=${sys.env("AMA_NODE")} && env MESOS_NATIVE_JAVA_LIBRARY=${conf.mesos.libPath}" +
          s" && python3 ${actionData.getSrc}"
      case "yarn" =>
          command + s" && /bin/bash spark/bin/load-spark-env.sh" +
                     s" && python3 ${actionData.getSrc}"
      case _ =>
          log.warn(s"Received unsupported cluster manager: ${conf.mode}")
          command
    }
  }

  override def getRunnerResources: Array[String] = {
    var resources = super.getRunnerResources
    resources = resources :+ s"amaterasu_pyspark-${conf.version}.zip"
    log.info(s"PYSPARK RESOURCES ==> ${resources.toSet}")
    resources
  }


  override def getHasExecutor: Boolean = true
}

object PySparkRunnerProvider {
  def apply(env: String, conf: ClusterConfig): PySparkRunnerProvider = {
    val result = new PySparkRunnerProvider(env, conf)
    result
  }
}