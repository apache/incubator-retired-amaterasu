package io.shinto.amaterasu.execution

import java.util.concurrent.{ ConcurrentHashMap, BlockingQueue }

import org.apache.curator.framework.CuratorFramework
import org.apache.zookeeper.CreateMode

import scala.collection._
import io.shinto.amaterasu.execution.actions.Action
import io.shinto.amaterasu.dataObjects.ActionData

import scala.collection.concurrent.TrieMap
import scala.collection.convert.decorateAsScala._

/**
  * The JobManager manages the lifecycle of a job. It queues new actions for execution,
  * tracks the state of actions and is in charge of communication with the underlying
  * cluster management framework (mesos)
  */
class JobManager {

  var name: String = null
  var jobId: String = null
  var client: CuratorFramework = null

  // TODO: this is not private due to tests, fix this!!!
  val registeredActions = new TrieMap[String, Action]
  private var executionQueue: BlockingQueue[ActionData] = null
  private val executingJobs: concurrent.Map[String, ActionData] = new ConcurrentHashMap[String, ActionData].asScala

  /**
    * The start method initiates the job execution by executing the first action.
    * start mast be called once and by the JobManager only
    */
  def start(): Unit = {

    registeredActions.head._2.execute()

  }

  /**
    * getNextActionData returns the data of the next action to be executed if such action
    * exists
    * @return the ActionData of the next action, returns null if no such action exists
    */
  def getNextActionData(): ActionData = {

    val nextAction: ActionData = executionQueue.poll()

    if (nextAction != null) {
      executingJobs.put(nextAction.id, nextAction)
    }

    nextAction
  }

  def reQueueAction(actionId: String): Unit = {

    val action = executingJobs.get(actionId)
    executionQueue.put(action.get)

  }

  /**
    * Registers an action with the job
    * @param action
    */
  def registerAction(action: Action): Unit = {

    registeredActions.put(action.actionId, action)

  }
}

object JobManager {

  /**
    * The apply method starts the job execution once the job is created from the maki.yaml file
    * it is in charge of creating the internal flow map, setting up ZooKeeper and executing
    * the first action
    * If the job execution is resumed (a job that was stooped) the init method will restore the
    * state of the job from ZooKepper
    * @param jobId
    * @param name
    * @param jobsQueue
    * @param client
    * @return
    */
  def apply(jobId: String, name: String, jobsQueue: BlockingQueue[ActionData], client: CuratorFramework): JobManager = {

    val manager = new JobManager()
    manager.name = name
    manager.executionQueue = jobsQueue

    manager.client = client
    client.create().withMode(CreateMode.PERSISTENT).forPath(s"/${jobId}")

    manager

  }

}