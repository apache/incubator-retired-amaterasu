package org.apache.amaterasu.frameworks.spark.dispatcher.runners.providers

import java.net.URLEncoder

import org.apache.amaterasu.common.configuration.ClusterConfig
import org.apache.amaterasu.common.dataobjects.ActionData
import org.apache.amaterasu.leader.common.utilities.DataLoader
import org.apache.amaterasu.sdk.frameworks.RunnerSetupProvider
import org.apache.hadoop.yarn.api.ApplicationConstants

class PySparkRunnerProvider extends RunnerSetupProvider {

  private var conf: ClusterConfig = _
  private val libPath = System.getProperty("java.library.path")

  override def getCommand(jobId: String, actionData: ActionData, env: String, executorId: String, callbackAddress: String): String = conf.mode match {
    case "mesos" =>
      s"env AMA_NODE=${sys.env("AMA_NODE")} env MESOS_NATIVE_JAVA_LIBRARY=/usr/lib/libmesos.so env SPARK_EXECUTOR_URI=http://${sys.env("AMA_NODE")}:${conf.Webserver.Port}/dist/spark-${conf.Webserver.sparkVersion}.tgz " +
      s"java -cp executor-${conf.version}-all.jar:spark-runner-${conf.version}-all.jar:spark-runtime-${conf.version}.jar:spark-${conf.Webserver.sparkVersion}/jars/* " +
      s"-Dscala.usejavacp=true -Djava.library.path=$libPath org.apache.amaterasu.executor.mesos.executors.MesosActionsExecutor $jobId ${conf.master} ${actionData.getName}.stripMargin"
    case "yarn" => "/bin/bash ./miniconda.sh -b -p $PWD/miniconda && " +
      s"/bin/bash spark/bin/load-spark-env.sh && " +
      s"java -cp spark/jars/*:executor.jar:spark-runner.jar:spark-runtime.jar:spark/conf/:${conf.YARN.hadoopHomeDir}/conf/ " +
      "-Xmx2G " +
      "-Dscala.usejavacp=true " +
      "-Dhdp.version=2.6.1.0-129 " +
      "org.apache.amaterasu.executor.yarn.executors.ActionsExecutorLauncher " +
      s"'$jobId' '${conf.master}' '${actionData.getName}' '${URLEncoder.encode(DataLoader.getTaskDataString(actionData, env), "UTF-8")}' '${URLEncoder.encode(DataLoader.getExecutorDataString(env, conf), "UTF-8")}' '$executorId' '$callbackAddress' " +
      s"1> ${ApplicationConstants.LOG_DIR_EXPANSION_VAR}/stdout " +
      s"2> ${ApplicationConstants.LOG_DIR_EXPANSION_VAR}/stderr "
    case _ => ""
  }

  override def getRunnerResources: Array[String] =
    Array[String]("miniconda.sh", "spark_intp.py", "runtime.py", "codegen.py")

  override def getActionUserResources(jobId: String, actionData: ActionData): Array[String] =
    Array[String]()

  override def getActionDependencies(jobId: String, actionData: ActionData): Array[String] =
    Array[String]()

  override def getHasExecutor: Boolean = true
}

object PySparkRunnerProvider {
  def apply(conf: ClusterConfig): PySparkRunnerProvider = {
    val result = new PySparkRunnerProvider
    result.conf = conf
    result
  }
}