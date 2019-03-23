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
package org.apache.amaterasu.executor.yarn.executors

import java.io.ByteArrayOutputStream
import java.net.{InetAddress, URLDecoder}

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.amaterasu.common.dataobjects.{ExecData, TaskData}
import org.apache.amaterasu.common.logging.Logging
import org.apache.amaterasu.executor.common.executors.{ActiveNotifier, ProvidersFactory}

import scala.collection.JavaConverters._


class ActionsExecutor extends Logging {

  var master: String = _
  var jobId: String = _
  var actionName: String = _
  var taskData: TaskData = _
  var execData: ExecData = _
  var providersFactory: ProvidersFactory = _

  def execute(): Unit = {
    val runner = providersFactory.getRunner(taskData.groupId, taskData.typeId)
    runner match {
      case Some(r) => {
        try {
          r.executeSource(taskData.src, actionName, taskData.exports.asJava)
          log.info("Completed action")
          System.exit(0)
        } catch {
          case e: Exception => {
            log.error("Exception in execute source", e)
            System.exit(100)
          }
        }
      }
      case None =>
        log.error("", s"Runner not found for group: ${taskData.groupId}, type ${taskData.typeId}. Please verify the tasks")
        System.exit(101)
    }
  }
}

// launched with args:
//s"'${jobManager.jobId}' '${config.master}' '${actionData.name}' '${URLEncoder.encode(gson.toJson(taskData), "UTF-8")}' '${URLEncoder.encode(gson.toJson(execData), "UTF-8")}' '${actionData.id}-${container.getId.getContainerId}'"
object ActionsExecutorLauncher extends Logging with App {

  val hostName = InetAddress.getLocalHost.getHostName

  log.info(s"Hostname resolved to: $hostName")
  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)

  log.info("Starting actions executor")

  val jobId = this.args(0)
  val master = this.args(1)
  val actionName = this.args(2)
  val notificationsAddress = this.args(6)

  log.info("parsing task data")
  val taskData = mapper.readValue(URLDecoder.decode(this.args(3), "UTF-8"), classOf[TaskData])
  log.info("parsing executor data")
  val execData = mapper.readValue(URLDecoder.decode(this.args(4), "UTF-8"), classOf[ExecData])
  val taskIdAndContainerId = this.args(5)

  val actionsExecutor: ActionsExecutor = new ActionsExecutor
  actionsExecutor.master = master
  actionsExecutor.actionName = actionName
  actionsExecutor.taskData = taskData
  actionsExecutor.execData = execData

  log.info("Setup executor")
  val baos = new ByteArrayOutputStream()
  val notifier = ActiveNotifier(notificationsAddress)

  notifier.info(s"Setup notifier for action $taskIdAndContainerId")
  actionsExecutor.providersFactory = ProvidersFactory(execData, jobId, baos, notifier, taskIdAndContainerId, hostName, propFile = "./amaterasu.properties")
  actionsExecutor.execute()
}
