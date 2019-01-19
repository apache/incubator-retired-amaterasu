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
package org.apache.amaterasu.executor.mesos.executors

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.amaterasu.common.dataobjects.{ExecData, TaskData}
import org.apache.amaterasu.common.logging.Logging
import org.apache.amaterasu.executor.common.executors.ProvidersFactory
import org.apache.mesos.Protos._
import org.apache.mesos.protobuf.ByteString
import org.apache.mesos.{Executor, ExecutorDriver, MesosExecutorDriver}

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success}

class MesosActionsExecutor extends Logging with Executor {

  var master: String = _
  var executorDriver: ExecutorDriver = _
  var jobId: String = _
  var actionName: String = _
  //  var sparkScalaRunner: SparkScalaRunner = _
  //  var pySparkRunner: PySparkRunner = _
  var notifier: MesosNotifier = _
  var providersFactory: ProvidersFactory = _

  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)


  override def shutdown(driver: ExecutorDriver) = {

  }

  override def killTask(driver: ExecutorDriver, taskId: TaskID) = ???

  override def disconnected(driver: ExecutorDriver) = ???

  override def reregistered(driver: ExecutorDriver, slaveInfo: SlaveInfo) = {
    this.executorDriver = driver
  }

  override def error(driver: ExecutorDriver, message: String) = {

    val status = TaskStatus.newBuilder
      .setData(ByteString.copyFromUtf8(message))
      .setState(TaskState.TASK_ERROR).build()

    driver.sendStatusUpdate(status)

  }

  override def frameworkMessage(driver: ExecutorDriver, data: Array[Byte]) = ???

  override def registered(driver: ExecutorDriver, executorInfo: ExecutorInfo, frameworkInfo: FrameworkInfo, slaveInfo: SlaveInfo): Unit = {

    this.executorDriver = driver
    val data = mapper.readValue(new ByteArrayInputStream(executorInfo.getData.toByteArray), classOf[ExecData])

    // this is used to resolve the spark drier address
    val hostName = slaveInfo.getHostname
    notifier = new MesosNotifier(driver)
    notifier.info(s"Executor ${executorInfo.getExecutorId.getValue} registered")
    val outStream = new ByteArrayOutputStream()
    providersFactory = ProvidersFactory(data, jobId, outStream, notifier, executorInfo.getExecutorId.getValue, hostName, "./amaterasu.properties")

  }

  override def launchTask(driver: ExecutorDriver, taskInfo: TaskInfo): Unit = {


    notifier.info(s"launching task: ${taskInfo.getTaskId.getValue}")
    log.debug(s"launching task: $taskInfo")
    val status = TaskStatus.newBuilder
      .setTaskId(taskInfo.getTaskId)
      .setState(TaskState.TASK_STARTING).build()
    driver.sendStatusUpdate(status)

    val task = Future {

      val taskData = mapper.readValue(new ByteArrayInputStream(taskInfo.getData.toByteArray), classOf[TaskData])

      val status = TaskStatus.newBuilder
        .setTaskId(taskInfo.getTaskId)
        .setState(TaskState.TASK_RUNNING).build()
      driver.sendStatusUpdate(status)
      val runner = providersFactory.getRunner(taskData.groupId, taskData.typeId)
      runner match {
        case Some(r) => r.executeSource(taskData.src, actionName, taskData.exports.asJava)
        case None =>
          notifier.error("", s"Runner not found for group: ${taskData.groupId}, type ${taskData.typeId}. Please verify the tasks")
          None
      }

    }

    task onComplete {

      case Failure(t) =>
        println(s"launching task Failed: ${t.getMessage}")
        System.exit(1)

      case Success(ts) =>

        driver.sendStatusUpdate(TaskStatus.newBuilder()
          .setTaskId(taskInfo.getTaskId)
          .setState(TaskState.TASK_FINISHED).build())
        notifier.info(s"Complete task: ${taskInfo.getTaskId.getValue}")

    }

  }

}

object MesosActionsExecutor extends Logging {

  def main(args: Array[String]) {
    System.loadLibrary("mesos")
    log.debug("Starting a new ActionExecutor")

    val executor = new MesosActionsExecutor
    executor.jobId = args(0)
    executor.master = args(1)
    executor.actionName = args(2)

    val driver = new MesosExecutorDriver(executor)
    driver.run()
  }

}
