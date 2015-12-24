package io.shinto.amaterasu.execution.actions

import java.util.concurrent.BlockingQueue

import io.shinto.amaterasu.enums.ActionStatus
import io.shinto.amaterasu.{ Config, Logging }
import io.shinto.amaterasu.dataObjects.ActionData

import org.apache.curator.framework.CuratorFramework
import org.apache.zookeeper.CreateMode

class SequentialAction extends Action with Logging {

  var jobId: String = null
  var jobsQueue: BlockingQueue[ActionData] = null

  var client: CuratorFramework = null
  var attempt: Int = 0

  def execute() = {

    try {

      announceQueued()
      jobsQueue.add(data)

    }
    catch {

      case e: Exception => handleFailure(attempt + 1, e)

    }

  }

  /**
    * The announceStart register the beginning of the of the task with ZooKeper
    */
  def announceStart(): Unit = {

    log.debug(s"Starting action ${data.name} of type ${data.actionType}")
    client.setData().forPath(actionPath, ActionStatus.started.toString.getBytes)

  }

  def announceQueued(): Unit = {

    log.debug(s"Action ${data.name} of type ${data.actionType} is queued for execution")
    client.setData().forPath(actionPath, ActionStatus.queued.toString.getBytes)

  }

  def announceComplete(): Unit = {

    log.debug(s"Action ${data.name} of type ${data.actionType} completed")
    client.setData().forPath(actionPath, ActionStatus.complete.toString.getBytes)
    //next.execute()

  }

  override def handleFailure(attemptNo: Int, e: Exception): Unit = {

    log.error(e.toString)
    log.debug(s"Part ${data.name} of type ${data.actionType} failed on attempt $attemptNo")
    attempt = attemptNo
    if (attempt <= 3) {

      //TODO: add retry policy
      execute()

    }
    else {

      announceFailure()

      //      if (error != null)
      //        error.execute()

    }

  }

}

object SequentialAction {

  def apply(name: String, src: String, jobType: String, jobId: String, queue: BlockingQueue[ActionData], zkClient: CuratorFramework): SequentialAction = {

    val action = new SequentialAction()

    action.jobsQueue = queue

    // creating a znode for the action
    action.client = zkClient
    action.actionPath = action.client.create().withMode(CreateMode.PERSISTENT_SEQUENTIAL).forPath(s"/${jobId}/task-", ActionStatus.pending.toString.getBytes())
    action.actionId = action.actionPath.substring(action.actionPath.indexOf('-') + 1)

    action.jobId = jobId
    action.data = new ActionData(name, src, jobType, action.actionId)
    action.jobsQueue = queue
    action.client = zkClient

    action
  }

}
