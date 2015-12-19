package io.shinto.amaterasu.mesos.schedulers

import java.util
import java.util.concurrent.{LinkedBlockingQueue, BlockingQueue}

import io.shinto.amaterasu.Logging
import io.shinto.amaterasu.dataObjects.ActionData
import io.shinto.amaterasu.execution.JobManager

import org.apache.mesos.Protos._
import org.apache.mesos.{Protos, SchedulerDriver, Scheduler}

/**
  * The JobScheduler is a mesos implementation. It is in charge of scheduling the execution of
  * Amaterasu actions for a specific job
  */
class JobScheduler extends Scheduler with Logging{

  //val actionsQueue: BlockingQueue[ActionData] = new LinkedBlockingQueue[ActionData]()
  val manager: JobManager = JobManager(null)

  def error(driver: SchedulerDriver, message: String) {}

  def executorLost(driver: SchedulerDriver, executorId: ExecutorID, slaveId: SlaveID, status: Int) {}

  def slaveLost(driver: SchedulerDriver, slaveId: SlaveID) {}

  def disconnected(driver: SchedulerDriver) {}

  def frameworkMessage(driver: SchedulerDriver, executorId: ExecutorID, slaveId: SlaveID, data: Array[Byte]) {}

  def statusUpdate(driver: SchedulerDriver, status: TaskStatus) = {
//    status.getState
//    TaskState.
  }

  def offerRescinded(driver: SchedulerDriver, offerId: OfferID) = {}

  def resourceOffers(driver: SchedulerDriver, offers: util.List[Offer]): Unit = {}

  def registered(driver: SchedulerDriver, frameworkId: FrameworkID, masterInfo: MasterInfo): Unit ={}

  def reregistered(driver: SchedulerDriver, masterInfo: Protos.MasterInfo) {}

}
