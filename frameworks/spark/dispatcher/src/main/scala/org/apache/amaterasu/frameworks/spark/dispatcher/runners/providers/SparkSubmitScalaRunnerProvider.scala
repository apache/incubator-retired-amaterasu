package org.apache.amaterasu.frameworks.spark.dispatcher.runners.providers

import org.apache.amaterasu.common.configuration.ClusterConfig
import org.apache.amaterasu.common.dataobjects.ActionData
import org.apache.amaterasu.common.utils.ArtifactUtil
import org.apache.amaterasu.sdk.frameworks.RunnerSetupProvider

import scala.collection.JavaConverters._

class SparkSubmitScalaRunnerProvider extends RunnerSetupProvider {

  private var conf: ClusterConfig = _

  override def getCommand(jobId: String, actionData: ActionData, env: String, executorId: String, callbackAddress: String): String ={
    val util = new ArtifactUtil(List(actionData.repo).asJava)

    s"$$SPARK_HOME/bin/spark-submit ${util.getLocalArtifacts(actionData.getArtifact).get(0)} --jars spark-runtime-${conf.version}.jar"
  }

  override def getRunnerResources: Array[String] =
    Array[String]()

  override def getActionUserResources(jobId: String, actionData: ActionData): Array[String] =
    Array[String]()

  override def getActionDependencies(jobId: String, actionData: ActionData): Array[String] = {
    val util = new ArtifactUtil(List(actionData.repo).asJava)
    util.getLocalArtifacts(actionData.getArtifact).toArray().map(_.toString)
  }

}

object SparkSubmitScalaRunnerProvider {
  def apply(conf: ClusterConfig): SparkSubmitScalaRunnerProvider = {
    val result = new SparkSubmitScalaRunnerProvider
    result.conf = conf
    result
  }
}