package org.apache.amaterasu.leader.frameworks.spark

import java.io.File

import org.apache.amaterasu.common.configuration.ClusterConfig
import org.apache.amaterasu.common.utils.FileUtils
import org.apache.amaterasu.sdk.FrameworkSetupProvider

import scala.collection.mutable

class SparkSetupProvider extends FrameworkSetupProvider {

  private var conf: ClusterConfig = _
  private val runnersResources = mutable.Map[String,Array[File]]()

  override def init(conf: ClusterConfig): Unit = {
    this.conf = conf

    runnersResources += "scala" -> Array.empty[File]
    runnersResources += "sql" -> Array.empty[File]
    //TODO: Nadav needs to setup conda here
    runnersResources += "python" -> Array.empty[File]
  }

  override def getGroupIdentifier: String = "spark"

  override def getGroupResources: Array[File] = {
    new File(conf.spark.home).listFiles
  }

  override def getRunnerResources(runnerId: String): Array[File] = {
    runnersResources(runnerId)
  }

}