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
package org.apache.amaterasu.leader.utilities

import java.io.{File, FileInputStream}
import java.nio.file.{Files, Paths}

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.amaterasu.common.configuration.ClusterConfig
import org.apache.amaterasu.common.dataobjects.{ActionData, ExecData, TaskData}
import org.apache.amaterasu.common.execution.dependencies.{Dependencies, PythonDependencies}
import org.apache.amaterasu.common.logging.Logging
import org.apache.amaterasu.common.runtime.Environment
import org.apache.mesos.protobuf.ByteString
import org.yaml.snakeyaml.Yaml

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.io.Source

/**
  * Created by karel_alfonso on 27/06/2016.
  */
object DataLoader extends Logging {

  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)

  val ymlMapper = new ObjectMapper(new YAMLFactory())
  ymlMapper.registerModule(DefaultScalaModule)

  def getTaskData(actionData: ActionData, env: String): ByteString = {

    val srcFile = actionData.src
    val src = Source.fromFile(s"repo/src/$srcFile").mkString
    val envValue = Source.fromFile(s"repo/env/$env/job.yml").mkString

    val envData = ymlMapper.readValue(envValue, classOf[Environment])

    val data = mapper.writeValueAsBytes(TaskData(src, envData, actionData.groupId, actionData.typeId, actionData.exports))
    ByteString.copyFrom(data)

  }

  def getExecutorData(env: String, clusterConf: ClusterConfig): ByteString = {
//    log.info("IN DATALOADER #1!!")
    // loading the job configuration
    val envValue = Source.fromFile(s"repo/env/$env/job.yml").mkString //TODO: change this to YAML
    val envData = ymlMapper.readValue(envValue, classOf[Environment])
//    log.info("IN DATALOADER #2!!")
    // loading all additional configurations
    val files = new File(s"repo/env/$env/").listFiles().filter(_.isFile).filter(_.getName != "job.yml")
    val config = files.map(yamlToMap).toMap
//    log.info("IN DATALOADER #3!!")
    // loading the job's dependencies
    var depsData: Dependencies = null
    var pyDepsData: PythonDependencies = null
//    log.info("IN DATALOADER #4!!")
    if (Files.exists(Paths.get("repo/deps/jars.yml"))) {
      val depsValue = Source.fromFile(s"repo/deps/jars.yml").mkString
      depsData = ymlMapper.readValue(depsValue, classOf[Dependencies])
    }
//    log.info("IN DATALOADER #5!!")
    if (Files.exists(Paths.get("repo/deps/python.yml"))) {
      val pyDepsValue = Source.fromFile(s"repo/deps/python.yml").mkString
//      log.info("IN DATALOADER #6!!")
//      log.info(s"pyDepsValue: ${pyDepsValue}")
      pyDepsData = ymlMapper.readValue(pyDepsValue, classOf[PythonDependencies])
//      log.info("IN DATALOADER #7!!")
//      log.info(s"pyDepsData: ${pyDepsData}")
    }
//    log.info("IN DATALOADER #8!!")
    val data = mapper.writeValueAsBytes(ExecData(envData, depsData, pyDepsData, config))
    ByteString.copyFrom(data)
  }

  def yamlToMap(file: File): (String, Map[String, Any]) = {

    val yaml = new Yaml()
    val conf = yaml.load(new FileInputStream(file)).asInstanceOf[java.util.Map[String, Any]].asScala.toMap

    (file.getName.replace(".yml",""), conf)
  }

}

class ConfMap[String,  T <: ConfMap[String, T]] extends mutable.ListMap[String, Either[String, T]]