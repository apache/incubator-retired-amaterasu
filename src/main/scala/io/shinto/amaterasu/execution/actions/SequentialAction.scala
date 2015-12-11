package io.shinto.amaterasu.execution.actions

import java.util.concurrent.BlockingQueue

import io.shinto.amaterasu.{ Config, Logging }
import io.shinto.amaterasu.dataObjects.ActionData

/**
  * Created by roadan on 12/5/15.
  */
class SequentialAction extends Action with Logging {

  //private var next: Part = null
  private var data: ActionData = null
  private var next: Action = null
  private var jobsQueue: BlockingQueue[ActionData] = null
  private var config: Config = null

  private var attempt: Int = 0

  def execute() = {

    try {

      announceStart()
      jobsQueue.add(data)

    }
    catch {

      case e: Exception => handleFailure(attempt + 1)

    }

  }

  def announceStart(): Unit = {

    log.debug(s"Starting part ${data.name} of type ${data.executingClass}")

  }

  def announceComplete(): Unit = {

    log.debug(s"Part ${data.name} of type ${data.executingClass} completed")
    next.execute()

  }

  override def handleFailure(attemptNo: Int): Unit = {

    log.debug(s"Part ${data.name} of type ${data.executingClass} failed on attempt $attemptNo")
    attempt = attemptNo
    if (attempt <= config.Jobs.Tasks.attempts)
      //TODO: add retry policy
      execute()
    else
      announceFailure()

  }

}

object SequentialAction {

  def apply(data: ActionData, config: Config, queue: BlockingQueue[ActionData]): SequentialAction = {

    val action = new SequentialAction()
    action.data = data
    action.config = config
    action.jobsQueue = queue

    action
  }

}
