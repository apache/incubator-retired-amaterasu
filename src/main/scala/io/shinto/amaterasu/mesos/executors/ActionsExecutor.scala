package io.shinto.amaterasu.mesos.executors

import java.io.ByteArrayInputStream

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import org.apache.mesos.protobuf.ByteString

import io.shinto.amaterasu.runtime.Environment
import io.shinto.amaterasu.Logging

import org.apache.mesos.Protos._
import org.apache.mesos.{ MesosExecutorDriver, ExecutorDriver, Executor }
import org.apache.spark.repl.amaterasu.runners.spark.SparkScalaRunner
import org.apache.spark.SparkContext

/**
  * Created by roadan on 1/1/16.
  */
class ActionsExecutor extends Executor with Logging {

  var master: String = _
  var executorDriver: ExecutorDriver = null
  var sc: SparkContext = null
  var jobId: String = null
  var actionName: String = null
  var sparkScalaRunner: SparkScalaRunner = null
  var notifier: MesosNotifier = null

  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)

  override def shutdown(driver: ExecutorDriver): Unit = {

  }

  override def disconnected(driver: ExecutorDriver): Unit = ???

  override def killTask(driver: ExecutorDriver, taskId: TaskID): Unit = ???

  override def reregistered(driver: ExecutorDriver, slaveInfo: SlaveInfo): Unit = {
    this.executorDriver = driver
  }

  override def error(driver: ExecutorDriver, message: String): Unit = {

    val status = TaskStatus.newBuilder
      .setData(ByteString.copyFromUtf8(message))
      .setState(TaskState.TASK_ERROR).build()

    driver.sendStatusUpdate(status)

  }

  override def frameworkMessage(driver: ExecutorDriver, data: Array[Byte]): Unit = ???

  override def registered(driver: ExecutorDriver, executorInfo: ExecutorInfo, frameworkInfo: FrameworkInfo, slaveInfo: SlaveInfo): Unit = {

    this.executorDriver = driver
    val env = mapper.readValue(new ByteArrayInputStream(executorInfo.getData.toByteArray), classOf[Environment])
    val sparkAppName = s"job_${jobId}_executor_${executorInfo.getExecutorId.getValue}"

    notifier = new MesosNotifier(driver)
    notifier.info(s"Executor ${executorInfo.getExecutorId.getValue} registered")

    sparkScalaRunner = SparkScalaRunner(env, jobId, sparkAppName, notifier)
  }

  override def launchTask(driver: ExecutorDriver, taskInfo: TaskInfo): Unit = {

    notifier.info(s"launching task: ${taskInfo.getTaskId.getValue}")
    log.debug(s"launching task: $taskInfo")
    val status = TaskStatus.newBuilder
      .setTaskId(taskInfo.getTaskId)
      .setState(TaskState.TASK_STARTING).build()

    driver.sendStatusUpdate(status)

    try {

      val taskData = mapper.readValue(new ByteArrayInputStream(taskInfo.getData.toByteArray), classOf[TaskData])

      val actionSource = taskData.src

      val status = TaskStatus.newBuilder
        .setTaskId(taskInfo.getTaskId)
        .setState(TaskState.TASK_RUNNING).build()

      driver.sendStatusUpdate(status)

      sparkScalaRunner.executeSource(actionSource, actionName)

      driver.sendStatusUpdate(TaskStatus.newBuilder()
        .setTaskId(taskInfo.getTaskId)
        .setState(TaskState.TASK_FINISHED).build())
      notifier.info(s"complete task: ${taskInfo.getTaskId.getValue}")
    }
    catch {
      case e: Exception => {
        println(s"launching task failed: ${e.getMessage}")

        System.exit(1)
      }
    }
  }

}

object ActionsExecutorLauncher extends Logging {

  def main(args: Array[String]) {
    System.loadLibrary("mesos")
    log.debug("Starting a new ActionExecutor")

    val executor = new ActionsExecutor
    executor.jobId = args(0)
    executor.master = args(1)
    executor.actionName = args(2)

    val driver = new MesosExecutorDriver(executor)
    driver.run()
  }

}