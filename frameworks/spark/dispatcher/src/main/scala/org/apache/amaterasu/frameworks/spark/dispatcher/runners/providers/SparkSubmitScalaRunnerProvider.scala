package org.apache.amaterasu.frameworks.spark.dispatcher.runners.providers

import java.io.File

import org.apache.amaterasu.common.configuration.ClusterConfig
import org.apache.amaterasu.common.dataobjects.ActionData
import org.apache.amaterasu.common.utils.ArtifactUtil
import org.apache.amaterasu.sdk.frameworks.RunnerSetupProvider

import scala.collection.JavaConverters._

class SparkSubmitScalaRunnerProvider extends RunnerSetupProvider {

  private var conf: ClusterConfig = _
  val jarFile = new File(this.getClass.getProtectionDomain.getCodeSource.getLocation.getPath)
  val amaDist = new File(s"${new File(jarFile.getParent).getParent}/dist")
  val amalocation = new File(s"${new File(jarFile.getParent).getParent}")

  override def getCommand(jobId: String, actionData: ActionData, env: String, executorId: String, callbackAddress: String): String = {

    val util = new ArtifactUtil(List(actionData.repo).asJava, jobId)
    val classParam = if (actionData.getHasArtifact) s" --class ${actionData.entryClass}" else ""
    s"$$SPARK_HOME/bin/spark-submit $classParam ${util.getLocalArtifacts(actionData.getArtifact).get(0).getName} --deploy-mode client --jars spark-runtime-${conf.version}.jar >&1"

  }

  override def getRunnerResources: Array[String] =
    Array[String]()

  override def getActionUserResources(jobId: String, actionData: ActionData): Array[String] = {
    Array[String]()
  }

  override def getActionDependencies(jobId: String, actionData: ActionData): Array[String] = {
    val util = new ArtifactUtil(List(actionData.repo).asJava, jobId)
    conf.mode match {
      case "mesos" => util.getLocalArtifacts(actionData.getArtifact).toArray().map(x => amaDist.toPath.relativize(x.asInstanceOf[File].toPath).toString)
      case "yarn" => util.getLocalArtifacts(actionData.getArtifact).toArray().map(x => x.asInstanceOf[File].getPath)
    }
  }

  override def getHasExecutor: Boolean = false

}

object SparkSubmitScalaRunnerProvider {
  def apply(conf: ClusterConfig): SparkSubmitScalaRunnerProvider = {
    val result = new SparkSubmitScalaRunnerProvider

    result.conf = conf
    result
  }
}