package io.shinto.amaterasu.executor.mesos.executors

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import io.shinto.amaterasu.common.execution.actions.{Notification, NotificationLevel, NotificationType, Notifier}
import io.shinto.amaterasu.common.logging.Logging
import org.apache.mesos.ExecutorDriver

/**
  * Created by roadan on 8/20/16.
  */
class MesosNotifier(driver: ExecutorDriver) extends Notifier with Logging {

  private val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)

  override def success(line: String) = {

    log.info(s"successfully executed line: $line")

    val notification = new Notification(line, "", NotificationType.success, NotificationLevel.code)
    val msg = mapper.writeValueAsBytes(notification)

    driver.sendFrameworkMessage(msg)

  }

  override def error(line: String, message: String) = {

    log.error(s"Error executing line: $line message: $message")

    val notification = new Notification(line, message, NotificationType.error, NotificationLevel.code)
    val msg = mapper.writeValueAsBytes(notification)

    driver.sendFrameworkMessage(msg)

  }

  override def info(message: String) = {

    log.info(message)

    val notification = new Notification("", message, NotificationType.info, NotificationLevel.execution)
    val msg = mapper.writeValueAsBytes(notification)

    driver.sendFrameworkMessage(msg)
  }
}