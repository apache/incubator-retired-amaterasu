package io.shinto.amaterasu.mesos.executors

import io.shinto.amaterasu.Logging
import org.apache.mesos.Protos._
import org.apache.mesos.{ ExecutorDriver, Executor }

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

object JobExecutor extends Executor with Logging {

  private val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)

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

    println("-----------------------> executing <--------------------------")

  }
}
