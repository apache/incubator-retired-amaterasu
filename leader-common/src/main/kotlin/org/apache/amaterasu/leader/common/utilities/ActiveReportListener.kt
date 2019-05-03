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


import com.beust.klaxon.Klaxon
import com.importre.crayon.bold
import com.importre.crayon.brightWhite
import com.importre.crayon.green
import com.importre.crayon.red
import org.apache.amaterasu.common.execution.actions.Notification
import org.apache.amaterasu.common.execution.actions.enums.NotificationType
import javax.jms.Message
import javax.jms.MessageListener
import javax.jms.TextMessage

class ActiveReportListener : MessageListener {

    override fun onMessage(message: Message): Unit = when (message) {
        is TextMessage -> try {
            val notification = Klaxon().parse<Notification>(message.text)
            notification?.let { printNotification(it) } ?: print("")

        } catch (e: Exception) {
            println(e.message)
        }
        else -> println("===> Unknown message")
    }

    private fun printNotification(notification: Notification) = when (notification.notType) {

        NotificationType.Info ->
            println("===> ${notification.msg} ".brightWhite().bold())
        NotificationType.Success ->
            println("===> ${notification.line}".green().bold())
        NotificationType.Error -> {
            println("===> ${notification.line}".red().bold())
            println("===> ${notification.msg} ".red().bold())

        }

    }

}




