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
  val amaDist = new File (s"${new File(jarFile.getParent).getParent}/dist")

  override def getCommand(jobId: String, actionData: ActionData, env: String, executorId: String, callbackAddress: String): String = {

    val util = new ArtifactUtil(List(actionData.repo).asJava, jobId)
    val classParam = if (actionData.getHasArtifact)  s" --class ${actionData.entryClass}" else ""
    s"spark-${conf.Webserver.sparkVersion}/bin/spark-submit $classParam ${util.getLocalArtifacts(actionData.getArtifact).get(0).getName} --deploy-mode client --jars spark-runtime-${conf.version}.jar >&1"
  }

  override def getRunnerResources: Array[String] =
    Array[String]()

  override def getActionUserResources(jobId: String, actionData: ActionData): Array[String] = {
    Array[String]()
  }

  override def getActionDependencies(jobId: String, actionData: ActionData): Array[String] = {
    val util = new ArtifactUtil(List(actionData.repo).asJava, jobId)
    util.getLocalArtifacts(actionData.getArtifact).toArray().map(x => amaDist.toPath.relativize(x.asInstanceOf[File].toPath).toString)
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