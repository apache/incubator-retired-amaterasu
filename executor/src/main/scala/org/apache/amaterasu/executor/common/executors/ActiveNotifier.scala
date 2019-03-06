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
package org.apache.amaterasu.executor.common.executors

import javax.jms.{DeliveryMode, MessageProducer, Session}
import net.liftweb.json._
import net.liftweb.json.Serialization.write
import org.apache.activemq.ActiveMQConnectionFactory
import org.apache.amaterasu.common.execution.actions.enums.{NotificationLevel, NotificationType}
import org.apache.amaterasu.common.execution.actions.{Notification, Notifier}
import org.apache.amaterasu.common.logging.Logging

class ActiveNotifier extends Notifier {

  var producer: MessageProducer = _
  var session: Session = _

  implicit val formats = DefaultFormats

  override def info(message: String): Unit = {

    getLog.info(message)

    val notification = new Notification("", message, NotificationType.Info, NotificationLevel.Execution)
    val notificationJson = write(notification)
    val msg = session.createTextMessage(notificationJson)
    producer.send(msg)

  }

  override def success(line: String): Unit = {

    getLog.info(s"successfully executed line: $line")

    val notification = new Notification(line, "", NotificationType.Success, NotificationLevel.Code)
    val notificationJson = write(notification)
    val msg = session.createTextMessage(notificationJson)
    producer.send(msg)

  }

  override def error(line: String, message: String): Unit = {

    getLog.error(s"Error executing line: $line message: $message")

    val notification = new Notification(line, message, NotificationType.Error, NotificationLevel.Code)
    val notificationJson = write(notification)
    val msg = session.createTextMessage(notificationJson)
    producer.send(msg)

  }
}

object ActiveNotifier extends Logging {
  def apply(address: String): ActiveNotifier = {

    // setting up activeMQ connection
    val connectionFactory = new ActiveMQConnectionFactory(address)
    val connection = connectionFactory.createConnection()
    connection.start()
    val session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
    val destination = session.createTopic("JOB.REPORT")
    val producer = session.createProducer(destination)
    producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT)

    // creating notifier
    val notifier = new ActiveNotifier
    notifier.session = session
    notifier.producer = producer

    notifier
  }
}