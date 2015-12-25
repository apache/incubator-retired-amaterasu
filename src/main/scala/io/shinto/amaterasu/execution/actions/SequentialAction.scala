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
    action.data = new ActionData(name, src, jobType, action.actionId, new ListBuffer[String], null)
    action.jobsQueue = queue
    action.client = zkClient

    action
  }

}
