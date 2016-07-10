package io.shinto.amaterasu.mesos.executors

import com.google.protobuf.ByteString
import io.shinto.amaterasu.Logging
import io.shinto.amaterasu.configuration.environments.Environment
import io.shinto.amaterasu.configuration.{ ClusterConfig, SparkConfig }
import io.shinto.amaterasu.execution.actions.runners.spark.SparkScalaRunner
import org.apache.mesos.Protos._
import org.apache.mesos.{ MesosExecutorDriver, ExecutorDriver, Executor }

/**
  * Created by roadan on 1/1/16.
  */
class ActionsExecutor extends Executor with Logging {

  var executorDriver: ExecutorDriver = null

  override def shutdown(driver: ExecutorDriver): Unit = ???

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
  }

  override def launchTask(driver: ExecutorDriver, taskInfo: TaskInfo): Unit = {

    log.debug(s"launching task: $taskInfo")
    val status = TaskStatus.newBuilder
      .setTaskId(taskInfo.getTaskId)
      .setState(TaskState.TASK_RUNNING).build()

    driver.sendStatusUpdate(status)
    val actionSource = taskInfo.getData().toStringUtf8()

    val jobId = "job-" + taskInfo.getTaskId.getValue
    val actionName = "action-" + taskInfo.getTaskId.getValue
    try {
      val env = Environment()
      env.workingDir = "file:///tmp/"
      env.master = "mesos://192.168.33.10:5050"

      val sparkScalaRunner = SparkScalaRunner(env, jobId)
      sparkScalaRunner.executeSource(actionSource, actionName)
      driver.sendStatusUpdate(TaskStatus.newBuilder()
        .setTaskId(taskInfo.getTaskId)
        .setState(TaskState.TASK_FINISHED).build())
    }
    catch {
      case e: Exception => {
        log.debug(s"launching task failed: ${e.getMessage}")

        System.exit(1)
      }
    }
  }

}

object ActionsExecutorLauncher extends Logging {

  def main(args: Array[String]) {
    System.loadLibrary("mesos")
    log.debug("Starting executor ------->")
    val driver = new MesosExecutorDriver(new ActionsExecutor)
    driver.run()
  }

}