package io.shinto.amaterasu.leader.execution.actions

import io.shinto.amaterasu.common.dataobjects.ActionData
import io.shinto.amaterasu.common.logging.Logging
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
    data.status = ActionStatus.started
  }

  def announceQueued: Unit = {

    log.debug(s"Action ${data.name} of type ${data.actionType} is queued for execution")
    client.setData().forPath(actionPath, ActionStatus.queued.toString.getBytes)
    data.status = ActionStatus.queued
  }

  def announceComplete: Unit = {

    log.debug(s"Action ${data.name} of type ${data.actionType} completed")
    client.setData().forPath(actionPath, ActionStatus.complete.toString.getBytes)
    data.status = ActionStatus.complete
  }

  def announceCanceled: Unit = {

    log.debug(s"Action ${data.name} of type ${data.actionType} was canceled")
    client.setData().forPath(actionPath, ActionStatus.canceled.toString.getBytes)
    data.status = ActionStatus.canceled
  }
  protected def announceFailure(): Unit = {}

}