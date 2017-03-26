package io.shinto.amaterasu.leader.mesos.executors

import io.shinto.amaterasu.common.logging.Logging
import org.apache.mesos.Protos._
import org.apache.mesos.{ ExecutorDriver, Executor }

object JobExecutor extends Executor with Logging {

  override def shutdown(driver: ExecutorDriver): Unit = {}

  override def disconnected(driver: ExecutorDriver): Unit = {}

  override def killTask(driver: ExecutorDriver, taskId: TaskID): Unit = {}

  override def reregistered(driver: ExecutorDriver, slaveInfo: SlaveInfo): Unit = {}

  override def error(driver: ExecutorDriver, message: String): Unit = {}

  override def frameworkMessage(driver: ExecutorDriver, data: Array[Byte]): Unit = {}

  override def registered(driver: ExecutorDriver, executorInfo: ExecutorInfo, frameworkInfo: FrameworkInfo, slaveInfo: SlaveInfo): Unit = {}

  override def launchTask(driver: ExecutorDriver, task: TaskInfo): Unit = {

    //val data = mapper.readValue(task.getData.toStringUtf8, JobData.getClass)

  }

  def main(args: Array[String]) {

    val repo = args(0)

  }
}
