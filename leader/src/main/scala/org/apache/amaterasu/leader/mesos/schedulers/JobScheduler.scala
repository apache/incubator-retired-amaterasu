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
package org.apache.amaterasu.leader.mesos.schedulers

import java.util
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.{ConcurrentHashMap, LinkedBlockingQueue}
import java.util.{Collections, UUID}

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.amaterasu.common.configuration.ClusterConfig
import org.apache.amaterasu.common.configuration.enums.ActionStatus
import org.apache.amaterasu.common.configuration.enums.ActionStatus.ActionStatus
import org.apache.amaterasu.common.dataobjects.ActionData
import org.apache.amaterasu.common.execution.actions.NotificationLevel.NotificationLevel
import org.apache.amaterasu.common.execution.actions.{Notification, NotificationLevel, NotificationType}
import org.apache.amaterasu.leader.execution.{JobLoader, JobManager}
import org.apache.amaterasu.leader.utilities.{DataLoader, HttpServer}
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.mesos.Protos.CommandInfo.URI
import org.apache.mesos.Protos._
import org.apache.mesos.protobuf.ByteString
import org.apache.mesos.{Protos, SchedulerDriver}

import scala.collection.JavaConverters._
import scala.collection.concurrent
import scala.collection.concurrent.TrieMap

/**
  * The JobScheduler is a Mesos implementation. It is in charge of scheduling the execution of
  * Amaterasu actions for a specific job
  */
class JobScheduler extends AmaterasuScheduler {

  private var jobManager: JobManager = _
  private var client: CuratorFramework = _
  private var config: ClusterConfig = _
  private var src: String = _
  private var env: String = _
  private var branch: String = _
  private var resume: Boolean = false
  private var reportLevel: NotificationLevel = _

  val slavesExecutors = new TrieMap[String, ExecutorInfo]
  private var awsEnv: String = ""

  // this map holds the following structure:
  // slaveId
  //  |
  //  +-> taskId, actionStatus)
  private val executionMap: concurrent.Map[String, concurrent.Map[String, ActionStatus]] = new ConcurrentHashMap[String, concurrent.Map[String, ActionStatus]].asScala
  private val lock = new ReentrantLock()
  private val offersToTaskIds: concurrent.Map[String, String] = new ConcurrentHashMap[String, String].asScala

  private val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)

  def error(driver: SchedulerDriver, message: String) {}

  def executorLost(driver: SchedulerDriver, executorId: ExecutorID, slaveId: SlaveID, status: Int) {}

  def slaveLost(driver: SchedulerDriver, slaveId: SlaveID) {}

  def disconnected(driver: SchedulerDriver) {}

  def frameworkMessage(driver: SchedulerDriver, executorId: ExecutorID, slaveId: SlaveID, data: Array[Byte]): Unit = {

    val notification = mapper.readValue(data, classOf[Notification])

    reportLevel match {
      case NotificationLevel.code => printNotification(notification)
      case NotificationLevel.execution =>
        if (notification.notLevel != NotificationLevel.code)
          printNotification(notification)
      case _ =>
    }

  }

  def statusUpdate(driver: SchedulerDriver, status: TaskStatus): Unit = {

    status.getState match {
      case TaskState.TASK_RUNNING => jobManager.actionStarted(status.getTaskId.getValue)
      case TaskState.TASK_FINISHED => jobManager.actionComplete(status.getTaskId.getValue)
      case TaskState.TASK_FAILED |
           TaskState.TASK_KILLED |
           TaskState.TASK_ERROR |
           TaskState.TASK_LOST => jobManager.actionFailed(status.getTaskId.getValue, status.getMessage) //TODO: revisit this
      case _ => log.warn("WTF? just got unexpected task state: " + status.getState)
    }

  }

  def validateOffer(offer: Offer): Boolean = {

    val resources = offer.getResourcesList.asScala

    resources.count(r => r.getName == "cpus" && r.getScalar.getValue >= config.Jobs.Tasks.cpus) > 0 &&
      resources.count(r => r.getName == "mem" && r.getScalar.getValue >= config.Jobs.Tasks.mem) > 0
  }

  def offerRescinded(driver: SchedulerDriver, offerId: OfferID): Unit = {

    val actionId = offersToTaskIds(offerId.getValue)
    jobManager.reQueueAction(actionId)

  }

  def resourceOffers(driver: SchedulerDriver, offers: util.List[Offer]): Unit = {
    for (offer <- offers.asScala) {

      if (validateOffer(offer)) {

        log.info(s"Accepting offer, id=${offer.getId}")

        // this is done to avoid the processing the same action
        // multiple times
        lock.lock()

        try {
          val actionData = jobManager.getNextActionData
          if (actionData != null) {
            val taskId = Protos.TaskID.newBuilder().setValue(actionData.id).build()

            offersToTaskIds.put(offer.getId.getValue, taskId.getValue)

            // atomically adding a record for the slave, I'm storing all the actions
            // on a slave level to efficiently handle slave loses
            executionMap.putIfAbsent(offer.getSlaveId.toString, new ConcurrentHashMap[String, ActionStatus].asScala)

            val slaveActions = executionMap(offer.getSlaveId.toString)
            slaveActions.put(taskId.getValue, ActionStatus.started)

            // searching for an executor that already exist on the slave, if non exist
            // we create a new one
            //TODO: move to .getOrElseUpdate when migrting to scala 2.11
            var executor: ExecutorInfo = null
            val slaveId = offer.getSlaveId.getValue
            slavesExecutors.synchronized {
              if (slavesExecutors.contains(slaveId) &&
                offer.getExecutorIdsList.contains(slavesExecutors(slaveId).getExecutorId)) {
                executor = slavesExecutors(slaveId)
              }
              else {
                val execData = DataLoader.getExecutorDataBytes(env, config)

                val command = CommandInfo
                  .newBuilder
                  .setValue(
                    s"""$awsEnv env AMA_NODE=${sys.env("AMA_NODE")} env MESOS_NATIVE_JAVA_LIBRARY=/usr/lib/libmesos.so env SPARK_EXECUTOR_URI=http://${sys.env("AMA_NODE")}:${config.Webserver.Port}/dist/spark-${config.Webserver.sparkVersion}.tgz java -cp executor-0.2.0-incubating-all.jar:spark-${config.Webserver.sparkVersion}/jars/* -Dscala.usejavacp=true -Djava.library.path=/usr/lib org.apache.amaterasu.executor.mesos.executors.ActionsExecutorLauncher ${jobManager.jobId} ${config.master} ${actionData.name}""".stripMargin
                  )
                  .addUris(URI.newBuilder
                    .setValue(s"http://${sys.env("AMA_NODE")}:${config.Webserver.Port}/executor-0.2.0-incubating-all.jar")
                    .setExecutable(false)
                    .setExtract(false)
                    .build())
                  .addUris(URI.newBuilder()
                    .setValue(s"http://${sys.env("AMA_NODE")}:${config.Webserver.Port}/spark-2.1.1-bin-hadoop2.7.tgz")
                    .setExecutable(false)
                    .setExtract(true)
                    .build())
                  .addUris(URI.newBuilder()
                    .setValue(s"http://${sys.env("AMA_NODE")}:${config.Webserver.Port}/Miniconda2-latest-Linux-x86_64.sh")
                    .setExecutable(false)
                    .setExtract(false)
                    .build())
                  .addUris(URI.newBuilder()
                    .setValue(s"http://${sys.env("AMA_NODE")}:${config.Webserver.Port}/spark_intp.py")
                    .setExecutable(false)
                    .setExtract(false)
                    .build())
                  .addUris(URI.newBuilder()
                    .setValue(s"http://${sys.env("AMA_NODE")}:${config.Webserver.Port}/runtime.py")
                    .setExecutable(false)
                    .setExtract(false)
                    .build())
                  .addUris(URI.newBuilder()
                    .setValue(s"http://${sys.env("AMA_NODE")}:${config.Webserver.Port}/codegen.py")
                    .setExecutable(false)
                    .setExtract(false)
                    .build())
                executor = ExecutorInfo
                  .newBuilder
                  .setData(ByteString.copyFrom(execData))
                  .setName(taskId.getValue)
                  .setExecutorId(ExecutorID.newBuilder().setValue(taskId.getValue + "-" + UUID.randomUUID()))
                  .setCommand(command)
                  .build()

                slavesExecutors.put(offer.getSlaveId.getValue, executor)
              }
            }

            val actionTask = TaskInfo
              .newBuilder
              .setName(taskId.getValue)
              .setTaskId(taskId)
              .setSlaveId(offer.getSlaveId)
              .setExecutor(executor)

              .setData(ByteString.copyFrom(DataLoader.getTaskDataBytes(actionData, env)))
              .addResources(createScalarResource("cpus", config.Jobs.Tasks.cpus))
              .addResources(createScalarResource("mem", config.Jobs.Tasks.mem))
              .addResources(createScalarResource("disk", config.Jobs.repoSize))
              .build()

            driver.launchTasks(Collections.singleton(offer.getId), Collections.singleton(actionTask))
          }
          else if (jobManager.outOfActions) {
            log.info(s"framework ${jobManager.jobId} execution finished")

            HttpServer.stop()
            driver.declineOffer(offer.getId)
            driver.stop()
          }
          else {
            log.info("Declining offer, no action ready for execution")
            driver.declineOffer(offer.getId)
          }
        }
        finally {
          lock.unlock()
        }
      }
      else {
        log.info("Declining offer, no sufficient resources")
        driver.declineOffer(offer.getId)
      }

    }

  }

  def registered(driver: SchedulerDriver, frameworkId: FrameworkID, masterInfo: MasterInfo): Unit = {

    if (!resume) {

      jobManager = JobLoader.loadJob(
        src,
        branch,
        frameworkId.getValue,
        client,
        config.Jobs.Tasks.attempts,
        new LinkedBlockingQueue[ActionData]()
      )
    }
    else {

      JobLoader.reloadJob(
        frameworkId.getValue,
        client,
        config.Jobs.Tasks.attempts,
        new LinkedBlockingQueue[ActionData]()
      )

    }
    jobManager.start()

  }

  def reregistered(driver: SchedulerDriver, masterInfo: Protos.MasterInfo) {}

  def printNotification(notification: Notification): Unit = {

    var color = Console.WHITE

    notification.notType match {

      case NotificationType.info =>
        color = Console.WHITE
        println(s"$color${Console.BOLD}===> ${notification.msg} ${Console.RESET}")
      case NotificationType.success =>
        color = Console.GREEN
        println(s"$color${Console.BOLD}===> ${notification.line} ${Console.RESET}")
      case NotificationType.error =>
        color = Console.RED
        println(s"$color${Console.BOLD}===> ${notification.line} ${Console.RESET}")
        println(s"$color${Console.BOLD}===> ${notification.msg} ${Console.RESET}")

    }

  }
}

object JobScheduler {

  def apply(src: String,
            branch: String,
            env: String,
            resume: Boolean,
            config: ClusterConfig,
            report: String,
            home: String): JobScheduler = {

    val scheduler = new JobScheduler()

    HttpServer.start(config.Webserver.Port, s"$home/${config.Webserver.Root}")

    if (!sys.env("AWS_ACCESS_KEY_ID").isEmpty &&
      !sys.env("AWS_SECRET_ACCESS_KEY").isEmpty) {

      scheduler.awsEnv = s"env AWS_ACCESS_KEY_ID=${sys.env("AWS_ACCESS_KEY_ID")} env AWS_SECRET_ACCESS_KEY=${sys.env("AWS_SECRET_ACCESS_KEY")}"
    }

    scheduler.resume = resume
    scheduler.src = src
    scheduler.branch = branch
    scheduler.env = env
    scheduler.reportLevel = NotificationLevel.withName(report)

    val retryPolicy = new ExponentialBackoffRetry(1000, 3)
    scheduler.client = CuratorFrameworkFactory.newClient(config.zk, retryPolicy)
    scheduler.client.start()
    scheduler.config = config
    scheduler

  }

}
