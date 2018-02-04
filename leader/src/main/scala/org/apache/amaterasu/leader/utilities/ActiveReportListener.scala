package org.apache.amaterasu.leader.utilities

import javax.jms.{Message, MessageListener, ObjectMessage}

import org.apache.amaterasu.common.execution.actions.{Notification, NotificationType}

class ActiveReportListener extends MessageListener {

  override def onMessage(message: Message): Unit = {
    message match {
      case om: ObjectMessage =>
        val notification = om.getObject.asInstanceOf[Notification]
        printNotification(notification)
    }
  }

  private  def printNotification(notification: Notification): Unit = {

    var color = Console.WHITE

    notification.notType match {

      case NotificationType.info =>
        color = Console.WHITE
        println(s"$color${Console.BOLD}===> ${notification.msg} ${Console.RESET}")
      case NotificationType.success =>
        color = Console.GREEN
        println(s"$color${Console.BOLD}===> ${notification.line} ${Console.RESET}")
      case NotificationType.error =>
        color = Console.RED
        println(s"$color${Console.BOLD}===> ${notification.line} ${Console.RESET}")
        println(s"$color${Console.BOLD}===> ${notification.msg} ${Console.RESET}")

    }

  }
}
