package io.shinto.amaterasu.mesos.executors

import com.fasterxml.jackson.databind.ObjectMapper
import io.shinto.amaterasu.Logging
import io.shinto.amaterasu.execution.actions.{ NotificationType, Notification, Notifier }

import org.apache.mesos.ExecutorDriver

/**
  * Created by roadan on 8/20/16.
  */
class MesosNotifier(driver: ExecutorDriver) extends Notifier with Logging {

  private val mapper = new ObjectMapper()
  override def success(line: String) = {

    log.info(s"successfully executed line: $line")

    val notification = new Notification(line, "", NotificationType.success)
    val msg = mapper.writeValueAsBytes(notification)
    driver.sendFrameworkMessage(msg)

  }

  override def error(line: String, message: String) = {

    log.error(s"Error executing line: $line message: $message")

    val notification = new Notification(line, message, NotificationType.error)
    val msg = mapper.writeValueAsBytes(notification)
    driver.sendFrameworkMessage(msg)

  }

  override def info(message: String) = {

    log.info(message)

    val notification = new Notification("", message, NotificationType.info)
    val msg = mapper.writeValueAsBytes(notification)
    driver.sendFrameworkMessage(msg)
  }
}