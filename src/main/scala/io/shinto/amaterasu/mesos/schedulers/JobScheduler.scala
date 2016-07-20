package io.shinto.amaterasu.mesos.schedulers

import java.util
import java.util.Collections
import java.util.concurrent.locks.ReentrantLock
import java.util.concurrent.{ ConcurrentHashMap, LinkedBlockingQueue }

import io.shinto.amaterasu.configuration.ClusterConfig
import io.shinto.amaterasu.dataObjects.ActionData
import io.shinto.amaterasu.enums.ActionStatus
import io.shinto.amaterasu.enums.ActionStatus.ActionStatus
import io.shinto.amaterasu.execution.{ JobLoader, JobManager }
import io.shinto.amaterasu.mesos.executors.TaskData
import io.shinto.amaterasu.utilities.FsUtil

import org.apache.curator.framework.{ CuratorFrameworkFactory, CuratorFramework }
import org.apache.curator.retry.ExponentialBackoffRetry

import org.apache.mesos.Protos.CommandInfo.URI
import org.apache.mesos.Protos._
import org.apache.mesos.{ Protos, SchedulerDriver }

import scala.collection.JavaConverters._
import scala.collection.concurrent

//import org.apache.mesos.protobuf.{ ByteString, GeneratedMessage }

/**
  * The JobScheduler is a mesos implementation. It is in charge of scheduling the execution of
  * Amaterasu actions for a specific job
  */
class JobScheduler extends AmaterasuScheduler {

  private var jobManager: JobManager = null
  private var client: CuratorFramework = null
  private var config: ClusterConfig = null
  private var src: String = null
  private var branch: String = null
  private var resume: Boolean = false

  // this map holds the following structure:
  // slaveId
  //  |
  //  +-> taskId, actionStatus)
  private val executionMap: concurrent.Map[String, concurrent.Map[String, ActionStatus]] = new ConcurrentHashMap[String, concurrent.Map[String, ActionStatus]].asScala
  private val lock = new ReentrantLock()
  private val offersToTaskIds: concurrent.Map[String, String] = new ConcurrentHashMap[String, String].asScala

  def error(driver: SchedulerDriver, message: String) {}

  def executorLost(driver: SchedulerDriver, executorId: ExecutorID, slaveId: SlaveID, status: Int) {}

  def slaveLost(driver: SchedulerDriver, slaveId: SlaveID) {}

  def disconnected(driver: SchedulerDriver) {}

  def frameworkMessage(driver: SchedulerDriver, executorId: ExecutorID, slaveId: SlaveID, data: Array[Byte]) {}

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

      log.debug(s"yumsudo service mesos-master start JobScheduler ${jobManager.jobId} : $offer")

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

            val fsUtil = FsUtil(config)

            val command = CommandInfo
              .newBuilder
              .setValue(
                //"""MESOS_NATIVE_JAVA_LIBRARY=/usr/lib/libmesos.so SPARK_EXECUTOR_URI=https://downloads.mesosphere.com/spark/assets/spark-1.6.1-1.tgz java -jar amaterasu-assembly-0.1.0.jar -classpath \"spark-assembly-1.6.1-hadoop2.4.0.jar\" -Djava.library.path=/usr/lib io.shinto.amaterasu.mesos.executors.ActionsExecutorLauncher""".stripMargin
                s"""env MESOS_NATIVE_JAVA_LIBRARY=/usr/lib/libmesos.so env SPARK_EXECUTOR_URI=http://192.168.33.11:8000/spark-assembly-1.6.2-hadoop2.4.0.tgz env SPARK_HOME="/home/vagrant/park-1.6.2-bin-hadoop2.4" java -cp amaterasu-assembly-0.1.0.jar:spark-assembly-1.6.2-hadoop2.4.0.jar:snappy-java-1.1.2.6.jar:hadoop-client-2.4.0.jar:hadoop-common-2.4.0.jar:conf/ -Dscala.usejavacp=true -Djava.library.path=/usr/lib io.shinto.amaterasu.mesos.executors.ActionsExecutorLauncher ${jobManager.jobId}""".stripMargin
              )
              .addUris(URI.newBuilder.setValue(fsUtil.getJarUrl()).setExecutable(false))
              .addUris(CommandInfo.URI.newBuilder()
                .setValue("http://127.0.0.1:8000/spark-assembly-1.6.2-hadoop2.4.0.jar")
                .setExecutable(false)
                .setExtract(false)
                .build())
            //              .addUris(CommandInfo.URI.newBuilder()
            //                .setValue("http://127.0.0.1:8000/snappy-java-1.1.2.6.jar")
            //                .setExecutable(false)
            //                .setExtract(false)
            //                .build())
            //              .addUris(CommandInfo.URI.newBuilder()
            //                .setValue("http://127.0.0.1:8000/hadoop-common-2.4.0.jar")
            //                .setExecutable(false)
            //                .setExtract(false)
            //                .build())
            //              .addUris(CommandInfo.URI.newBuilder()
            //                .setValue("http://127.0.0.1:8000/hadoop-client-2.4.0.jar")
            //                .setExecutable(false)
            //                .setExtract(false)
            //                .build())
            //              .addUris(CommandInfo.URI.newBuilder()
            //                .setValue("http://127.0.0.1:8000/conf.tar.gz")
            //                .setExecutable(false)
            //                .setExtract(true)
            //                .build())

            val executor = ExecutorInfo
              .newBuilder
              .setName(taskId.getValue)
              .setExecutorId(ExecutorID.newBuilder().setValue("1234")) //TODO: fix this
              .setCommand(command)

            val actionTask = TaskInfo
              .newBuilder
              .setName(taskId.getValue)
              .setTaskId(taskId)
              .setSlaveId(offer.getSlaveId)
              .setExecutor(executor)
              .setData(new TaskData(actionData).toTaskData())
              .addResources(createScalarResource("cpus", config.Jobs.Tasks.cpus))
              .addResources(createScalarResource("mem", config.Jobs.Tasks.mem))
              .addResources(createScalarResource("disk", config.Jobs.repoSize))
              .build()

            driver.launchTasks(Collections.singleton(offer.getId), Collections.singleton(actionTask))
          }
          else if (jobManager.outOfActions) {
            log.info(s"framework ${jobManager.jobId} execution finished")

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

}

object JobScheduler {

  def apply(src: String, branch: String, env: String, resume: Boolean, config: ClusterConfig): JobScheduler = {

    val scheduler = new JobScheduler()
    scheduler.resume = resume
    scheduler.src = src
    scheduler.branch = branch

    val retryPolicy = new ExponentialBackoffRetry(1000, 3)
    scheduler.client = CuratorFrameworkFactory.newClient(config.zk, retryPolicy)
    scheduler.client.start()

    scheduler.config = config
    scheduler

  }

}