package io.shinto.amaterasu.execution.actions

import java.util.concurrent.BlockingQueue

import io.shinto.amaterasu.enums.ActionStatus
import io.shinto.amaterasu.{ Config, Logging }
import io.shinto.amaterasu.dataObjects.ActionData

import org.apache.curator.framework.CuratorFramework
import org.apache.zookeeper.CreateMode

class SequentialAction extends Action with Logging {

  private var data: ActionData = null
  private var jobsQueue: BlockingQueue[ActionData] = null
  private var config: Config = null

  private var next: Action = null
  private var error: Action = null

  private var client: CuratorFramework = null
  private var attempt: Int = 0

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

    log.debug(s"Starting action ${data.name} of type ${data.executingClass}")
    client.create().withMode(CreateMode.PERSISTENT_SEQUENTIAL).forPath(s"/${data.jobId}/task-")

  }

  def announceQueued(): Unit = {

    log.debug(s"Action ${data.name} of type ${data.executingClass} is queued for execution")
    client.create().withMode(CreateMode.PERSISTENT_SEQUENTIAL).forPath(s"/${data.jobId}/task-", ActionStatus.queued.toString.getBytes())

  }

  def announceComplete(): Unit = {

    log.debug(s"Action ${data.name} of type ${data.executingClass} completed")
    next.execute()

  }

  override def handleFailure(attemptNo: Int, e: Exception): Unit = {

    log.error(e.toString)
    log.debug(s"Part ${data.name} of type ${data.executingClass} failed on attempt $attemptNo")
    attempt = attemptNo
    if (attempt <= config.Jobs.Tasks.attempts){

      //TODO: add retry policy
      execute()

    } else {

      announceFailure()

      if(error != null)
        error.execute()

    }

  }

}

object SequentialAction {

  def apply(data: ActionData, config: Config, queue: BlockingQueue[ActionData], client: CuratorFramework, next: Action, error: Action): SequentialAction = {

    val action = new SequentialAction()
    action.data = data
    action.config = config
    action.jobsQueue = queue
    action.client = client
    action.next = next
    action.error = error

    action
  }

}
