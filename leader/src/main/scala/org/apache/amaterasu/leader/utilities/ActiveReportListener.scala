package org.apache.amaterasu.leader.utilities

import javax.jms.{Message, MessageListener, TextMessage}

import net.liftweb.json._
import net.liftweb.json.JsonDSL._
import org.apache.amaterasu.common.execution.actions.{Notification, NotificationLevel, NotificationType}

class ActiveReportListener extends MessageListener {

  implicit val formats = DefaultFormats

  override def onMessage(message: Message): Unit = {
    message match {
      case tm: TextMessage =>
        try {
          val notification = parseNot(parse(tm.getText))
          printNotification(notification)

        } catch {
          case e: Exception => println(e.getMessage)
        }
      case _ => println("===> Unknown message")
    }
  }

  private def parseNot(json: JValue): Notification = Notification(
    (json \ "line").asInstanceOf[JString].values,
    (json \ "msg").asInstanceOf[JString].values,
    NotificationType.withName((json \ "notType" \ "name").asInstanceOf[JString].values),
    NotificationLevel.withName((json \ "notLevel" \ "name").asInstanceOf[JString].values)
  )


  private def printNotification(notification: Notification): Unit = {

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

