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

import java.io.{File, PrintWriter, StringWriter}
import java.util
import java.util.UUID
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.{ConcurrentHashMap, LinkedBlockingQueue}

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.amaterasu.common.configuration.ClusterConfig
import org.apache.amaterasu.common.configuration.enums.ActionStatus
import org.apache.amaterasu.common.dataobjects.ActionData
import org.apache.amaterasu.common.execution.actions.Notification
import org.apache.amaterasu.common.execution.actions.enums.{NotificationLevel, NotificationType}
import org.apache.amaterasu.leader.common.configuration.ConfigManager
import org.apache.amaterasu.leader.common.execution.JobManager
import org.apache.amaterasu.leader.common.execution.frameworks.FrameworkProvidersFactory
import org.apache.amaterasu.leader.common.utilities.DataLoader
import org.apache.amaterasu.leader.execution.JobLoader
import org.apache.amaterasu.leader.utilities.HttpServer
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.log4j.LogManager
import org.apache.mesos.Protos.CommandInfo.URI
import org.apache.mesos.Protos.Environment.Variable
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

  /*private val props: Properties = new Properties(new File(""))
  private val version = props.getProperty("version")
  println(s"===> version  $version")*/
  LogManager.resetConfiguration()
  private var frameworkFactory: FrameworkProvidersFactory = _
  private var configManager: ConfigManager = _
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

  private val yamlMapper = new ObjectMapper(new YAMLFactory())
  yamlMapper.registerModule(DefaultScalaModule)

  def error(driver: SchedulerDriver, message: String) {}

  def executorLost(driver: SchedulerDriver, executorId: ExecutorID, slaveId: SlaveID, status: Int) {}

  def slaveLost(driver: SchedulerDriver, slaveId: SlaveID) {}

  def disconnected(driver: SchedulerDriver) {}

  def frameworkMessage(driver: SchedulerDriver, executorId: ExecutorID, slaveId: SlaveID, data: Array[Byte]): Unit = {

        val notification = mapper.readValue(data, classOf[Notification])

        reportLevel match {
          case NotificationLevel.Code => printNotification(notification)
          case NotificationLevel.Execution =>
            if (notification.getNotLevel != NotificationLevel.Code)
              printNotification(notification)
          case _ =>
        }

  }

  def statusUpdate(driver: SchedulerDriver, status: TaskStatus): Unit = {

    status.getState match {
      case TaskState.TASK_STARTING => log.info("Task starting ...")
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

    println(jobManager.toString)

    for (offer <- offers.asScala) {

      if (validateOffer(offer)) {

        log.info(s"Accepting offer, id=${offer.getId}")

        // this is done to avoid the processing the same action
        // multiple times
        lock.lock()

        try {
          val actionData = jobManager.getNextActionData
          if (actionData != null) {
            val taskId = Protos.TaskID.newBuilder().setValue(actionData.getId).build()

            // setting up the configuration files for the container
            val envYaml = configManager.getActionConfigContent(actionData.getName, actionData.getConfig)
            writeConfigFile(envYaml, jobManager.getJobId, actionData.getName, "env.yaml")

            val dataStores = DataLoader.getTaskData(actionData, env).exports
            val writer = new StringWriter()
            yamlMapper.writeValue(writer, dataStores)
            val dataStoresYaml = writer.toString
            writeConfigFile(dataStoresYaml, jobManager.getJobId, actionData.getName, "datastores.yaml")

            writeConfigFile(s"jobId: ${jobManager.getJobId}\nactionName: ${actionData.getName}", jobManager.getJobId, actionData.getName, "runtime.yaml")

            offersToTaskIds.put(offer.getId.getValue, taskId.getValue)

            // atomically adding a record for the slave, I'm storing all the actions
            // on a slave level to efficiently handle slave loses
            executionMap.putIfAbsent(offer.getSlaveId.toString, new ConcurrentHashMap[String, ActionStatus].asScala)

            val slaveActions = executionMap(offer.getSlaveId.toString)
            slaveActions.put(taskId.getValue, ActionStatus.Started)


            val frameworkProvider = frameworkFactory.providers(actionData.getGroupId)
            val runnerProvider = frameworkProvider.getRunnerProvider(actionData.getTypeId)

            // searching for an executor that already exist on the slave, if non exist
            // we create a new one
            var executor: ExecutorInfo = null

            //            val slaveId = offer.getSlaveId.getValue
            //            slavesExecutors.synchronized {

            val execData = DataLoader.getExecutorDataBytes(env, config)
            val executorId = taskId.getValue + "-" + UUID.randomUUID()
            //creating the command

            //                          // TODO: move this into the runner provider somehow
            //                          if(!actionData.getSrc.isEmpty){
            //                            copy(get(s"repo/src/${actionData.getSrc}"), get(s"dist/${jobManager.getJobId}/${actionData.getName}/${actionData.getSrc}"), REPLACE_EXISTING)
            //                          }
            val commandStr = runnerProvider.getCommand(jobManager.getJobId, actionData, env, executorId, "")
            val command = CommandInfo
              .newBuilder
              .setValue(commandStr)
              .addUris(URI.newBuilder
                .setValue(s"http://${sys.env("AMA_NODE")}:${config.Webserver.Port}/executor-${config.version}-all.jar")
                .setExecutable(false)
                .setExtract(false)
                .build())

            // Getting framework (group) resources
            frameworkProvider.getGroupResources.foreach(f => command.addUris(URI.newBuilder
              .setValue(s"http://${sys.env("AMA_NODE")}:${config.Webserver.Port}/${f.getName}")
              .setExecutable(false)
              .setExtract(true)
              .build()
            ))

            // Getting runner resources
            runnerProvider.getRunnerResources.foreach(r => {
              command.addUris(URI.newBuilder
                .setValue(s"http://${sys.env("AMA_NODE")}:${config.Webserver.Port}/$r")
                .setExecutable(false)
                .setExtract(false)
                .build())
            }
            )

            // Getting action dependencies
            runnerProvider.getActionDependencies(jobManager.getJobId, actionData).foreach(r => {
              command.addUris(URI.newBuilder
                .setValue(s"http://${sys.env("AMA_NODE")}:${config.Webserver.Port}/$r")
                .setExecutable(false)
                .setExtract(false)
                .build())
            }
            )

            // Getting action specific resources
            runnerProvider.getActionResources(jobManager.getJobId, actionData).foreach(r => command.addUris(URI.newBuilder
              .setValue(s"http://${sys.env("AMA_NODE")}:${config.Webserver.Port}/$r")
              .setExecutable(false)
              .setExtract(false)
              .build()))

            command
              .addUris(URI.newBuilder()
                .setValue(s"http://${sys.env("AMA_NODE")}:${config.Webserver.Port}/miniconda.sh") //TODO: Nadav needs to clean this on the executor side
                .setExecutable(true)
                .setExtract(false)
                .build())
              .addUris(URI.newBuilder()
                .setValue(s"http://${sys.env("AMA_NODE")}:${config.Webserver.Port}/amaterasu.properties")
                .setExecutable(false)
                .setExtract(false)
                .build())

            // setting the processes environment variables
            val envVarsList = frameworkProvider.getEnvironmentVariables.asScala.toList.map(x => Variable.newBuilder().setName(x._1).setValue(x._2).build()).asJava
            command.setEnvironment(Environment.newBuilder().addAllVariables(envVarsList))

            executor = ExecutorInfo
              .newBuilder
              .setData(ByteString.copyFrom(execData))
              .setName(taskId.getValue)
              .setExecutorId(ExecutorID.newBuilder().setValue(executorId))
              .setCommand(command)

              .build()

            slavesExecutors.put(offer.getSlaveId.getValue, executor)


            val driverConfiguration = frameworkProvider.getDriverConfiguration

            var actionTask: TaskInfo = null

            if (runnerProvider.getHasExecutor) {
              actionTask = TaskInfo
                .newBuilder
                .setName(taskId.getValue)
                .setTaskId(taskId)
                .setExecutor(executor)

                .setData(ByteString.copyFrom(DataLoader.getTaskDataBytes(actionData, env)))
                .addResources(createScalarResource("cpus", driverConfiguration.getCpus))
                .addResources(createScalarResource("mem", driverConfiguration.getMemory))
                .addResources(createScalarResource("disk", config.Jobs.repoSize))
                .setSlaveId(offer.getSlaveId)
                .build()

              //driver.launchTasks(Collections.singleton(offer.getId), List(actionTask).asJava)
            }
            else {
              actionTask = TaskInfo
                .newBuilder
                .setName(taskId.getValue)
                .setTaskId(taskId)
                .setCommand(command)

                //.setData(ByteString.copyFrom(DataLoader.getTaskDataBytes(actionData, env)))
                .addResources(createScalarResource("cpus", driverConfiguration.getCpus))
                .addResources(createScalarResource("mem", driverConfiguration.getMemory))
                .addResources(createScalarResource("disk", config.Jobs.repoSize))
                .setSlaveId(offer.getSlaveId)
                .build()

              //driver.launchTasks(Collections.singleton(offer.getId), List(actionTask).asJava)
            }
            driver.launchTasks(offer.getId, List(actionTask).asJava)

          }
          else if (jobManager.getOutOfActions) {
            log.info(s"framework ${jobManager.getJobId} execution finished")

            val repo = new File("repo/")
            repo.delete()

            HttpServer.stop()
            driver.declineOffer(offer.getId)
            driver.stop()
            sys.exit()
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

    frameworkFactory = FrameworkProvidersFactory(env, config)
    val items = frameworkFactory.providers.values.flatMap(_.getConfigurationItems).toList.asJava
    configManager = new ConfigManager(env, "repo", items)

    jobManager.start()

    createJobDir(jobManager.getJobId)

  }

  def reregistered(driver: SchedulerDriver, masterInfo: Protos.MasterInfo) {}

  def printNotification(notification: Notification): Unit = {

    var color = Console.WHITE

    notification.getNotType match {

      case NotificationType.Info =>
        color = Console.WHITE
        println(s"$color${Console.BOLD}===> ${notification.getMsg} ${Console.RESET}")
      case NotificationType.Success =>
        color = Console.GREEN
        println(s"$color${Console.BOLD}===> ${notification.getLine} ${Console.RESET}")
      case NotificationType.Error =>
        color = Console.RED
        println(s"$color${Console.BOLD}===> ${notification.getLine} ${Console.RESET}")
        println(s"$color${Console.BOLD}===> ${notification.getMsg} ${Console.RESET}")

    }

  }

  private def createJobDir(jobId: String): Unit = {
    val jarFile = new File(this.getClass.getProtectionDomain.getCodeSource.getLocation.getPath)
    val amaHome = new File(jarFile.getParent).getParent
    val jobDir = s"$amaHome/dist/$jobId/"

    val dir = new File(jobDir)
    if (!dir.exists()) {
      dir.mkdir()
    }
  }

  /**
    * This function creates an action specific env.yml file int the dist folder with the following path:
    * dist/{jobId}/{actionName}/env.yml to be added to the container
    *
    * @param configuration A YAML string to be written to the env file
    * @param jobId         the jobId
    * @param actionName    the name of the action
    */
  def writeConfigFile(configuration: String, jobId: String, actionName: String, fileName: String): Unit = {
    val jarFile = new File(this.getClass.getProtectionDomain.getCodeSource.getLocation.getPath)
    val amaHome = new File(jarFile.getParent).getParent
    val envLocation = s"$amaHome/dist/$jobId/$actionName/"

    val dir = new File(envLocation)
    if (!dir.exists()) {
      dir.mkdir()
    }

    new PrintWriter(s"$envLocation/$fileName") {
      write(configuration)
      close
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

    LogManager.resetConfiguration()
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
    scheduler.reportLevel = NotificationLevel.valueOf(report.capitalize)

    val retryPolicy = new ExponentialBackoffRetry(1000, 3)
    scheduler.client = CuratorFrameworkFactory.newClient(config.zk, retryPolicy)
    scheduler.client.start()
    scheduler.config = config

    scheduler

  }

}