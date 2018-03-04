package org.apache.amaterasu.leader.frameworks.spark

import java.io.File

import org.apache.amaterasu.common.configuration.ClusterConfig
import org.apache.amaterasu.common.dataobjects.ExecData
import org.apache.amaterasu.leader.utilities.DataLoader
import org.apache.amaterasu.sdk.frameworks.FrameworkSetupProvider
import org.apache.amaterasu.sdk.frameworks.configuration.DriverConfiguration

import scala.collection.mutable

class SparkSetupProvider extends FrameworkSetupProvider {


  private var env: String = _
  private var conf: ClusterConfig = _
  private val runnersResources = mutable.Map[String,Array[File]]()
  private var execData: ExecData = _
  private var sparkExecConfigurations = mutable.Map[String, Any]()

  override def init(env: String, conf: ClusterConfig): Unit = {
    this.env = env
    this.conf = conf
    this.execData = DataLoader.getExecutorData(env, conf)
    val sparkExecConfigurationsurations = execData.configurations.get("spark")
    if (sparkExecConfigurationsurations.isEmpty) {
      throw new Exception(s"Spark configuration files could not be loaded for the environment ${env}")
    }
    this.sparkExecConfigurations = sparkExecConfigurations ++ sparkExecConfigurationsurations.get

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

  override def getDriverConfiguration: DriverConfiguration = {
    var cpu: Int = 0
    if (sparkExecConfigurations.get("spark.yarn.am.cores").isDefined) {
      cpu = sparkExecConfigurations.get("spark.yarn.am.cores").toString.toInt
    } else if (sparkExecConfigurations.get("spark.driver.cores").isDefined) {
      cpu = sparkExecConfigurations.get("spark.yarn.am.cores").toString.toInt
    } else if (conf.spark.opts.contains("yarn.am.cores")) {
      cpu = conf.spark.opts("yarn.am.cores").toInt
    } else if (conf.spark.opts.contains("driver.cores")) {
      cpu = conf.spark.opts("driver.cores").toInt
    } else if (conf.YARN.Worker.cores > 0) {
      cpu = conf.YARN.Worker.cores
    } else {
      cpu = 1
    }
    var mem: Int = 0
    if (sparkExecConfigurations.get("spark.yarn.am.memory").isDefined) {
      mem = sparkExecConfigurations.get("spark.yarn.am.memory").toString.toInt
    } else if (sparkExecConfigurations.get("spark.driver.memeory").isDefined) {
      mem = sparkExecConfigurations.get("spark.yarn.am.memory").toString.toInt
    } else if (conf.spark.opts.contains("yarn.am.memory")) {
      mem = conf.spark.opts("yarn.am.memory").toInt
    } else if (conf.spark.opts.contains("driver.memory")) {
      mem = conf.spark.opts("driver.memory").toInt
    } else if (conf.YARN.Worker.memoryMB > 0) {
      mem = conf.YARN.Worker.memoryMB
    } else if (conf.taskMem > 0) {
      mem = conf.taskMem
    } else {
      mem = 1024
    }

    new DriverConfiguration(mem, cpu)
  }
}