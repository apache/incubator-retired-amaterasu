package org.apache.amaterasu.executor.common.executors

import javax.jms.{DeliveryMode, MessageProducer, Session}

import org.apache.activemq.ActiveMQConnectionFactory
import org.apache.amaterasu.common.execution.actions.{Notification, NotificationLevel, NotificationType, Notifier}
import org.apache.amaterasu.common.logging.Logging

class ActiveNotifier extends Notifier with Logging {

  var producer: MessageProducer = _
  var session: Session = _

  override def info(message: String): Unit = {

    log.info(message)

    val notification = Notification("", message, NotificationType.info, NotificationLevel.execution)
    val msg = session.createObjectMessage(notification)
    producer.send(msg)

  }

  override def success(line: String): Unit = {

    log.info(s"successfully executed line: $line")

    val notification =  Notification(line, "", NotificationType.success, NotificationLevel.code)
    val msg = session.createObjectMessage(notification)
    producer.send(msg)

  }

  override def error(line: String, message: String): Unit = {

    log.error(s"Error executing line: $line message: $message")

    val notification =  Notification(line, message, NotificationType.error, NotificationLevel.code)
    val msg = session.createObjectMessage(notification)
    producer.send(msg)

  }
}

object ActiveNotifier extends Logging {
  def apply(address: String) : ActiveNotifier = {

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