package io.shinto.amaterasu.execution.actions

import java.util.concurrent.BlockingQueue

import io.shinto.amaterasu.enums.ActionStatus
import io.shinto.amaterasu.dataObjects.ActionData

import org.apache.curator.framework.CuratorFramework
import org.apache.zookeeper.CreateMode

import scala.collection.mutable.ListBuffer

class SequentialAction extends Action {

  var jobId: String = null
  var jobsQueue: BlockingQueue[ActionData] = null
  var attempts: Int = 3
  var attempt: Int = 1

  def execute() = {

    try {

      announceQueued
      jobsQueue.add(data)

    }
    catch {

      //TODO: this will not invoke the error action
      case e: Exception => handleFailure(e.getMessage)

    }

  }

  override def handleFailure(message: String): String = {

    log.error(message)
    log.debug(s"Part ${data.name} of type ${data.actionType} failed on attempt $attempt")
    attempt += 1

    if (attempt <= attempts) {
      data.id
    }
    else {
      announceFailure()
      data.errorActionId
    }

  }

}

object SequentialAction {

  def apply(
    name: String,
    src: String,
    jobType: String,
    jobId: String,
    queue: BlockingQueue[ActionData],
    zkClient: CuratorFramework,
    attempts: Int
  ): SequentialAction = {

    val action = new SequentialAction()

    action.jobsQueue = queue

    // creating a znode for the action
    action.client = zkClient
    action.actionPath = action.client.create().withMode(CreateMode.PERSISTENT_SEQUENTIAL).forPath(s"/${jobId}/task-", ActionStatus.pending.toString.getBytes())
    action.actionId = action.actionPath.substring(action.actionPath.indexOf("task-") + 5)

    action.attempts = attempts
    action.jobId = jobId
    action.data = new ActionData(name, src, jobType, action.actionId, new ListBuffer[String])
    action.jobsQueue = queue
    action.client = zkClient

    action
  }

}

object ErrorAction {

  def apply(
    name: String,
    src: String,
    parent: String,
    jobType: String,
    jobId: String,
    queue: BlockingQueue[ActionData],
    zkClient: CuratorFramework
  ): SequentialAction = {

    val action = new SequentialAction()

    action.jobsQueue = queue

    // creating a znode for the action
    action.client = zkClient
    action.actionPath = action.client.create().withMode(CreateMode.PERSISTENT).forPath(s"/${jobId}/task-$parent-error", ActionStatus.pending.toString.getBytes())
    action.actionId = action.actionPath.substring(action.actionPath.indexOf('-') + 1)

    action.jobId = jobId
    action.data = new ActionData(name, src, jobType, action.actionId, new ListBuffer[String])
    action.jobsQueue = queue
    action.client = zkClient

    action

  }
}