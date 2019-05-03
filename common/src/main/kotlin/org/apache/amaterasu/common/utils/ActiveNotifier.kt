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
package org.apache.amaterasu.common.utils

import org.apache.activemq.ActiveMQConnectionFactory
import org.apache.amaterasu.common.execution.actions.Notification
import org.apache.amaterasu.common.execution.actions.Notifier
import org.apache.amaterasu.common.execution.actions.enums.NotificationLevel
import org.apache.amaterasu.common.execution.actions.enums.NotificationType
import org.codehaus.jackson.map.ObjectMapper
import javax.jms.DeliveryMode
import javax.jms.MessageProducer
import javax.jms.Session

class ActiveNotifier(address: String) : Notifier() {

    private val mapper = ObjectMapper()

    private var producer: MessageProducer
    private var session: Session

    init {
        log.info("report address $address")

        // setting up activeMQ connection
        val connectionFactory = ActiveMQConnectionFactory(address)
        val connection = connectionFactory.createConnection()
        connection.start()
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)
        val destination = session.createTopic("JOB.REPORT")
        producer = session.createProducer(destination)
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT)

        //mapper.registerModule(KotlinModule())
    }

    override fun info(msg: String) {

        val notification = Notification("", msg, NotificationType.Info, NotificationLevel.Execution)
        val notificationJson = mapper.writeValueAsString(notification)

        log.info(notificationJson)
        val message = session.createTextMessage(notificationJson)
        producer.send(message)

    }

    override fun error(line: String, msg: String) {

        println("Error executing line: $line message: $msg")

        val notification = Notification(line, msg, NotificationType.Error, NotificationLevel.Code)
        val notificationJson = mapper.writeValueAsString(notification)
        val message = session.createTextMessage(notificationJson)
        producer.send(message)
    }

    override fun success(line: String) {

        log.info("successfully executed line: $line")

        val notification = Notification(line, "", NotificationType.Success, NotificationLevel.Code)
        val notificationJson = mapper.writeValueAsString(notification)
        val message = session.createTextMessage(notificationJson)
        producer.send(message)
    }
}