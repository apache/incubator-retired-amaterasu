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
package org.apache.amaterasu.leader.frameworks.spark

import java.io.File

import org.apache.amaterasu.common.configuration.ClusterConfig
import org.apache.amaterasu.leader.utilities.{DataLoader, MemoryFormatParser}
import org.apache.amaterasu.sdk.frameworks.FrameworkSetupProvider
import org.apache.amaterasu.sdk.frameworks.configuration.DriverConfiguration

import scala.collection.mutable

class SparkSetupProvider extends FrameworkSetupProvider {

  //private var execData: ExecData = _
  private lazy val sparkExecConfigurations: mutable.Map[String, Any] = loadSparkConfig
  private val runnersResources = mutable.Map[String, Array[File]]()
  private var env: String = _
  private var conf: ClusterConfig = _

  override def init(env: String, conf: ClusterConfig): Unit = {
    this.env = env
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

  override def getRuntimePaths: Array[String] = {
    Array[String](
      s"executor-${conf.version}-all.jar",
      s"spark-${conf.Webserver.sparkVersion}/jars/*"
    )
  }

  override def getRuntimeCommand = {
    "java -cp"
  }

  private def loadSparkConfig: mutable.Map[String, Any] = {
    val execData = DataLoader.getExecutorData(env, conf)
    val sparkExecConfigurationsurations = execData.configurations.get("spark")
    if (sparkExecConfigurationsurations.isEmpty) {
      throw new Exception(s"Spark configuration files could not be loaded for the environment ${env}")
    }
    collection.mutable.Map(sparkExecConfigurationsurations.get.toSeq: _*)
  }
}