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
package org.apache.amaterasu.executor.mesos.executors

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.amaterasu.common.execution.actions.{Notification, NotificationLevel, NotificationType, Notifier}
import org.apache.amaterasu.common.logging.Logging
import org.apache.mesos.ExecutorDriver


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