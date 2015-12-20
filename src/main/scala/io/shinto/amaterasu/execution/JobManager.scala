package io.shinto.amaterasu.execution

import java.util.concurrent.{ ConcurrentHashMap, LinkedBlockingQueue, BlockingQueue }
import java.util.{ List => List }

import scala.collection.convert.decorateAsScala._
import scala.collection._
import com.fasterxml.jackson.annotation.JsonProperty

import io.shinto.amaterasu.execution.actions.SequentialAction
import io.shinto.amaterasu.dataObjects.{ JobData, ActionData }

/**
  * The JobManager manages the lifecycle of a job. It queues new actions for execution,
  * tracks the state of actions and is in charge of communication with the underlying
  * cluster management framework (mesos)
  */
class JobManager(
    @JsonProperty("job-name") jobName: String,
    @JsonProperty("flow") jobFlow: List[SequentialAction]
) {

  val flow: List[SequentialAction] = jobFlow
  val name: String = jobName

  private var jobsQueue: BlockingQueue[ActionData] = null
  private var executingJobs: concurrent.Map[String, ActionData] = null

  /**
    * getNextActionData returns the data of the next action to be executed if such action
    * exists
    * @return the ActionData of the next action, returns null if no such action exists
    */
  def getNextActionData(): ActionData = {

    val nextAction: ActionData = jobsQueue.poll()

    if (nextAction != null) {
      executingJobs.put(nextAction.id, nextAction)
    }

    nextAction
  }
}

//object JobManager {
//
//  def apply(data: JobData, name: String): JobManager = {
//
//    val manager = new JobManager()
//    manager.jobsQueue = new LinkedBlockingQueue[ActionData]()
//    manager.executingJobs = new ConcurrentHashMap[String, ActionData].asScala
//
//    manager
//  }
//
//}