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
package org.apache.amaterasu.leader.common.utilities

import org.apache.activemq.ActiveMQConnectionFactory
import java.net.InetAddress
import java.net.ServerSocket
import javax.jms.MessageConsumer
import javax.jms.Session

object MessagingClientUtil {

    @JvmStatic
    fun setupMessaging(brokerURL: String): MessageConsumer {

        val cf = ActiveMQConnectionFactory(brokerURL)
        val conn = cf.createConnection()
        conn.start()

        val session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE)
        //TODO: move to a const in common
        val destination = session.createTopic("JOB.REPORT")

        val consumer = session.createConsumer(destination)
        consumer.messageListener = ActiveReportListener()

        return consumer
    }

    private fun generatePort(): Int {
        val socket = ServerSocket(0)
        val port = socket.localPort
        socket.close()
        return port
    }

    @JvmStatic
    val borkerAddress: String
        get() {
            val host: String = InetAddress.getLocalHost().hostName
            return "tcp://$host:${MessagingClientUtil.generatePort()}"
        }
}