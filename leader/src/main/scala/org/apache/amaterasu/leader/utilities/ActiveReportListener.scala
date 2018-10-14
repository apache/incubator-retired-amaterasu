/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.amaterasu.leader.utilities

import javax.jms.{Message, MessageListener, TextMessage}
import net.liftweb.json._
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

