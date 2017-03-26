package io.shinto.amaterasu.common.execution.actions

import io.shinto.amaterasu.common.execution.actions.NotificationLevel.NotificationLevel
import io.shinto.amaterasu.common.execution.actions.NotificationType.NotificationType

/**
  * Created by roadan on 8/20/16.
  */
abstract class Notifier {

  def info(msg: String)

  def success(line: String)

  def error(line: String, msg: String)

}


object NotificationType extends Enumeration {

  type NotificationType = Value
  val success = Value("success")
  val error = Value("error")
  val info = Value("info")

}

object NotificationLevel extends Enumeration {

  type NotificationLevel = Value
  val execution = Value("execution")
  val code = Value("code")
  val none = Value("none")

}

case class Notification(line: String,
                        msg: String,
                        notType: NotificationType,
                        notLevel: NotificationLevel)
