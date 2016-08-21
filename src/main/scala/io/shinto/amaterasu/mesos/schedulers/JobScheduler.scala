package io.shinto.amaterasu.mesos.schedulers

import java.util
import java.util.{ UUID, Collections }
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.{ ConcurrentHashMap, LinkedBlockingQueue }

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import io.shinto.amaterasu.Logging
import io.shinto.amaterasu.configuration.ClusterConfig
import io.shinto.amaterasu.dataObjects.ActionData
import io.shinto.amaterasu.enums.ActionStatus
import io.shinto.amaterasu.enums.ActionStatus.ActionStatus
import io.shinto.amaterasu.execution.actions.{ NotificationType, Notification, NotificationLevel }
import io.shinto.amaterasu.execution.actions.NotificationLevel.NotificationLevel
import io.shinto.amaterasu.execution.{ JobLoader, JobManager }
import io.shinto.amaterasu.mesos.executors.TaskDataLoader

import org.apache.curator.framework.{ CuratorFrameworkFactory, CuratorFramework }
import org.apache.curator.retry.ExponentialBackoffRetry

import org.apache.mesos.Protos.CommandInfo.URI
import org.apache.mesos.Protos._
import org.apache.mesos.{ Protos, SchedulerDriver }

import scala.collection.JavaConverters._
import scala.collection.concurrent

/**
  * The JobScheduler is a mesos implementation. It is in charge of scheduling the execution of
  * Amaterasu actions for a specific job
  */
class JobScheduler extends AmaterasuScheduler {

  private var jobManager: JobManager = null
  private var client: CuratorFramework = null
  private var config: ClusterConfig = null
  private var src: String = null
  private var env: String = null
  private var branch: String = null
  private var resume: Boolean = false
  private var reportLevel: NotificationLevel = _
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

  def frameworkMessage(driver: SchedulerDriver, executorId: ExecutorID, slaveId: SlaveID, data: Array[Byte]) = {

    val notification = mapper.readValue(data, classOf[Notification])

    reportLevel match {
      case NotificationLevel.code => printNotification(notification)
      case NotificationLevel.execution =>
        if (notification.notLevel != NotificationLevel.code)
          printNotification(notification)
      case _ =>
    }

  }

  def statusUpdate(driver: SchedulerDriver, status: TaskStatus) = {

    status.getState match {
      case TaskState.TASK_RUNNING  => jobManager.actionStarted(status.getTaskId.getValue)
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

  def offerRescinded(driver: SchedulerDriver, offerId: OfferID) = {

    val actionId = offersToTaskIds.get(offerId.getValue).get
    jobManager.reQueueAction(actionId)

  }

  def resourceOffers(driver: SchedulerDriver, offers: util.List[Offer]): Unit = {

    for (offer <- offers.asScala) {

      log.debug(s"start JobScheduler ${jobManager.jobId} : $offer")

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

            val slaveActions = executionMap.get(offer.getSlaveId.toString).get
            slaveActions.put(taskId.getValue, ActionStatus.started)

            val command = CommandInfo
              .newBuilder
              .setValue(
                s"""$awsEnv env AMA_NODE=${sys.env("AMA_NODE")} env MESOS_NATIVE_JAVA_LIBRARY=/usr/lib/libmesos.so env SPARK_EXECUTOR_URI=http://${sys.env("AMA_NODE")}:8000/spark-1.6.1-2.tgz env SPARK_HOME="~/spark-1.6.1-bin-hadoop2.4" java -cp amaterasu-assembly-0.1.0.jar:spark-assembly-1.6.1-hadoop2.4.0.jar -Dscala.usejavacp=true -Djava.library.path=/usr/lib io.shinto.amaterasu.mesos.executors.ActionsExecutorLauncher ${jobManager.jobId} ${config.master} ${actionData.name}""".stripMargin
              )
              .addUris(URI.newBuilder
                .setValue(s"http://${sys.env("AMA_NODE")}:8000/amaterasu-assembly-0.1.0.jar")
                .setExecutable(false)
                .setExtract(false)
                .build())
              .addUris(URI.newBuilder()
                .setValue(s"http://${sys.env("AMA_NODE")}:8000/spark-assembly-1.6.1-hadoop2.4.0.jar")
                .setExecutable(false)
                .setExtract(false)
                .build())

            val executor = ExecutorInfo
              .newBuilder
              .setName(taskId.getValue)
              .setExecutorId(ExecutorID.newBuilder().setValue(taskId.getValue + "-" + UUID.randomUUID()))
              .setCommand(command)

            val actionTask = TaskInfo
              .newBuilder
              .setName(taskId.getValue)
              .setTaskId(taskId)
              .setSlaveId(offer.getSlaveId)
              .setExecutor(executor)
              .setData(new TaskDataLoader(actionData, env).toTaskData())
              .addResources(createScalarResource("cpus", config.Jobs.Tasks.cpus))
              .addResources(createScalarResource("mem", config.Jobs.Tasks.mem))
              .addResources(createScalarResource("disk", config.Jobs.repoSize))
              .build()

            driver.launchTasks(Collections.singleton(offer.getId), Collections.singleton(actionTask))
          }
          else if (jobManager.outOfActions) {
            log.info(s"framework ${jobManager.jobId} execution finished")

            jobManager.jobReport.append("******************************************************************")
            log.info(jobManager.jobReport.result)
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

  def printNotification(notification: Notification) = {

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

  def apply(
    src: String,
    branch: String,
    env: String,
    resume: Boolean,
    config: ClusterConfig,
    report: String
  ): JobScheduler = {

    val scheduler = new JobScheduler()
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