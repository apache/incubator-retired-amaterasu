/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.amaterasu.frameworks.spark.dispatcher

import java.io.File
import java.util

import org.apache.amaterasu.common.configuration.ClusterConfig
import org.apache.amaterasu.frameworks.spark.dispatcher.runners.providers._
import org.apache.amaterasu.leader.common.utilities.{DataLoader, MemoryFormatParser}
import org.apache.amaterasu.sdk.frameworks.configuration.DriverConfiguration
import org.apache.amaterasu.sdk.frameworks.{FrameworkSetupProvider, RunnerSetupProvider}

import scala.collection.mutable
import collection.JavaConversions._

class SparkSetupProvider extends FrameworkSetupProvider {

  private var env: String = _
  private var conf: ClusterConfig = _
  private lazy val sparkExecConfigurations: mutable.Map[String, Any] = loadSparkConfig

  private val runnerProviders: mutable.Map[String, RunnerSetupProvider] = mutable.Map[String, RunnerSetupProvider]()

  private def loadSparkConfig: mutable.Map[String, Any] = {

    val execData = DataLoader.getExecutorData(env, conf)
    val sparkExecConfiguration = execData.configurations.get("spark")
    if (sparkExecConfiguration.isEmpty) {
      throw new Exception(s"Spark configuration files could not be loaded for the environment $env")
    }
    collection.mutable.Map(sparkExecConfiguration.get.toSeq: _*)

  }

  override def init(env: String, conf: ClusterConfig): Unit = {
    this.env = env
    this.conf = conf

    runnerProviders += ("scala" -> SparkScalaRunnerProvider(conf))
    runnerProviders += ("jar" -> SparkSubmitScalaRunnerProvider(conf))
    runnerProviders += ("pyspark" -> PySparkRunnerProvider(conf))

  }

  override def getGroupIdentifier: String = "spark"

  override def getGroupResources: Array[File] = conf.mode match {
      case "mesos" => Array[File](new File(s"spark-${conf.Webserver.sparkVersion}.tgz"), new File(s"spark-runner-${conf.version}-all.jar"), new File(s"spark-runtime-${conf.version}.jar"))
      case "yarn" => new File(conf.spark.home).listFiles
      case _ => Array[File]()
    }


  override def getEnvironmentVariables: util.Map[String, String] = conf.mode match {
    case "mesos" => Map[String, String]("SPARK_HOME" ->s"spark-${conf.Webserver.sparkVersion}","SPARK_HOME_DOCKER" -> "/opt/spark/")
    case "yarn" => Map[String, String]("SPARK_HOME" -> "spark")
    case _ => Map[String, String]()
  }

  override def getDriverConfiguration: DriverConfiguration = {
    var cpu: Int = 0
    if (sparkExecConfigurations.get("spark.yarn.am.cores").isDefined) {
      cpu = sparkExecConfigurations("spark.yarn.am.cores").toString.toInt
    } else if (sparkExecConfigurations.get("spark.driver.cores").isDefined) {
      cpu = sparkExecConfigurations("spark.driver.cores").toString.toInt
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
      mem = MemoryFormatParser.extractMegabytes(sparkExecConfigurations("spark.yarn.am.memory").toString)
    } else if (sparkExecConfigurations.get("spark.driver.memeory").isDefined) {
      mem = MemoryFormatParser.extractMegabytes(sparkExecConfigurations("spark.driver.memeory").toString)
    } else if (conf.spark.opts.contains("yarn.am.memory")) {
      mem = MemoryFormatParser.extractMegabytes(conf.spark.opts("yarn.am.memory"))
    } else if (conf.spark.opts.contains("driver.memory")) {
      mem = MemoryFormatParser.extractMegabytes(conf.spark.opts("driver.memory"))
    } else if (conf.YARN.Worker.memoryMB > 0) {
      mem = conf.YARN.Worker.memoryMB
    } else if (conf.taskMem > 0) {
      mem = conf.taskMem
    } else {
      mem = 1024
    }

    new DriverConfiguration(mem, cpu)
  }

  override def getRunnerProvider(runnerId: String): RunnerSetupProvider = {
    runnerProviders(runnerId)
  }

  override def getConfigurationItems = Array("sparkConfiguration", "sparkExecutor")
}