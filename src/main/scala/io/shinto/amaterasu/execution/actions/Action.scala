package io.shinto.amaterasu.execution.actions

import io.shinto.amaterasu.Logging
import io.shinto.amaterasu.dataObjects.ActionData
import io.shinto.amaterasu.enums.ActionStatus
import org.apache.curator.framework.CuratorFramework

trait Action extends Logging {

  // this is the znode path for the action
  var actionPath: String = _
  var actionId: String = _

  var data: ActionData = null
  var client: CuratorFramework = null

  def execute(): Unit

  def handleFailure(message: String): String

  /**
    * The announceStart register the beginning of the of the task with ZooKeper
    */
  def announceStart: Unit = {

    log.debug(s"Starting action ${data.name} of type ${data.actionType}")
    client.setData().forPath(actionPath, ActionStatus.started.toString.getBytes)

  }

  def announceQueued: Unit = {

    log.debug(s"Action ${data.name} of type ${data.actionType} is queued for execution")
    client.setData().forPath(actionPath, ActionStatus.queued.toString.getBytes)

  }

  def announceComplete: Unit = {

    log.debug(s"Action ${data.name} of type ${data.actionType} completed")
    client.setData().forPath(actionPath, ActionStatus.complete.toString.getBytes)

  }

  protected def announceFailure(): Unit = {}

}