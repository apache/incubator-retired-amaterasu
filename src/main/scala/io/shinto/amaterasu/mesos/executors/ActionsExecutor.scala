package io.shinto.amaterasu.mesos.executors

import io.shinto.amaterasu.Logging
import io.shinto.amaterasu.configuration.SparkConfig
import io.shinto.amaterasu.execution.actions.runners.spark.SparkScalaRunner
import org.apache.mesos.Protos._
import org.apache.mesos.{ MesosExecutorDriver, ExecutorDriver, Executor }

/**
  * Created by roadan on 1/1/16.
  */
class ActionsExecutor extends Executor with Logging {

  override def shutdown(driver: ExecutorDriver): Unit = ???

  override def disconnected(driver: ExecutorDriver): Unit = ???

  override def killTask(driver: ExecutorDriver, taskId: TaskID): Unit = ???

  override def reregistered(driver: ExecutorDriver, slaveInfo: SlaveInfo): Unit = ???

  override def error(driver: ExecutorDriver, message: String): Unit = ???

  override def frameworkMessage(driver: ExecutorDriver, data: Array[Byte]): Unit = ???

  override def registered(driver: ExecutorDriver, executorInfo: ExecutorInfo, frameworkInfo: FrameworkInfo, slaveInfo: SlaveInfo): Unit = {

  }

  override def launchTask(driver: ExecutorDriver, task: TaskInfo): Unit = {
    val status = TaskStatus.newBuilder
      .setTaskId(task.getTaskId)
      .setState(TaskState.TASK_RUNNING).build()

    driver.sendStatusUpdate(status)
    val actionType = System.getProperty("action.type")
    val actionSource = System.getProperty("action.source")

    val jobId = "job-" + task.getTaskId.getValue
    val actionName = "action-" + task.getTaskId.getValue
    val sparkScalaRunner = SparkScalaRunner(new SparkConfig(), actionType, jobId)
    val sparkContext = null
    sparkScalaRunner.execute(actionSource, sparkContext, actionName)
  }

}

object ActionsExecutorLauncher extends Logging {

  def main(args: Array[String]) {

    log.debug("Starting executor ------->")
    val driver = new MesosExecutorDriver(new ActionsExecutor)
    driver.run()
    //System.exit(if (driver.run eq Status.DRIVER_STOPPED) 0 else 1)

  }

}