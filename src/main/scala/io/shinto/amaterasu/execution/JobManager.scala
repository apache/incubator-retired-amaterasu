package io.shinto.amaterasu.execution

import java.util.concurrent.BlockingQueue

import org.apache.curator.framework.CuratorFramework
import org.apache.zookeeper.CreateMode

import io.shinto.amaterasu.execution.actions.Action
import io.shinto.amaterasu.dataObjects.ActionData
import io.shinto.amaterasu.Logging

import scala.collection.concurrent.TrieMap

/**
  * The JobManager manages the lifecycle of a job. It queues new actions for execution,
  * tracks the state of actions and is in charge of communication with the underlying
  * cluster management framework (mesos)
  */
class JobManager extends Logging {

  var name: String = null
  var jobId: String = null
  var client: CuratorFramework = null

  // TODO: this is not private due to tests, fix this!!!
  val registeredActions = new TrieMap[String, Action]
  private var executionQueue: BlockingQueue[ActionData] = null

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
    *
    * @return the ActionData of the next action, returns null if no such action exists
    */
  def getNextActionData: ActionData = {

    val nextAction: ActionData = executionQueue.poll()

    if (nextAction != null) {
      registeredActions.get(nextAction.id).get.announceStart
    }

    nextAction
  }

  def reQueueAction(actionId: String): Unit = {

    val action = registeredActions.get(actionId).get
    executionQueue.put(action.data)
    registeredActions.get(actionId).get.announceQueued

  }

  /**
    * Registers an action with the job
    *
    * @param action
    */
  def registerAction(action: Action): Unit = {

    registeredActions.put(action.actionId, action)

  }

  /**
    * announce the completion of an action and executes the next actions
    *
    * @param actionId
    */
  def actionComplete(actionId: String): Unit = {

    val action = registeredActions.get(actionId).get
    action.announceComplete
    action.data.nextActionIds.foreach(id =>
      registeredActions.get(id).get.execute())
  }

  /**
    * gets the next action id which can be either the same action or an error action
    * and if it exist (we have an error action or a retry)
    *
    * @param actionId
    */
  def actionFailed(actionId: String, message: String): Unit = {

    log.warn(message)
    val action = registeredActions.get(actionId).get
    val id = action.handleFailure(message)
    if (id != null)
      registeredActions.get(id).get.execute()

  }

  /**
    * announce the start of execution of the action
    */
  def actionStarted(actionId: String): Unit = {

    val action = registeredActions.get(actionId).get
    action.announceStart

  }
}

object JobManager {

  /**
    * The apply method starts the job execution once the job is created from the maki.yaml file
    * it is in charge of creating the internal flow map, setting up ZooKeeper and executing
    * the first action
    * If the job execution is resumed (a job that was stooped) the init method will restore the
    * state of the job from ZooKepper
    *
    * @param jobId
    * @param name
    * @param jobsQueue
    * @param client
    * @return
    */
  def apply(
    jobId: String,
    name: String,
    jobsQueue: BlockingQueue[ActionData],
    client: CuratorFramework
  ): JobManager = {

    val manager = new JobManager()
    manager.name = name
    manager.executionQueue = jobsQueue
    manager.jobId = jobId
    manager.client = client

    manager

  }

}