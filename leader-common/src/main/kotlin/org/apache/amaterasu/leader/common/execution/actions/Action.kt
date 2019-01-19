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
package org.apache.amaterasu.leader.common.execution.actions

import org.apache.amaterasu.common.dataobjects.ActionData
import org.apache.amaterasu.common.configuration.enums.ActionStatus
import org.apache.amaterasu.common.logging.KLogging
import org.apache.curator.framework.CuratorFramework

/**
 * Created by Eran Bartenstein on 19/10/18.
 */
abstract class Action : KLogging() {
    lateinit var actionPath: String
    lateinit var actionId: String
    lateinit var client: CuratorFramework
    lateinit var data: ActionData
    abstract fun execute()
    abstract fun handleFailure(message: String) : String

    fun announceStart() {
        log.debug("Starting action ${data.name} of group ${data.groupId} and type ${data.typeId}")
        client.setData().forPath(actionPath, ActionStatus.Started.value.toByteArray())
        data.status = ActionStatus.Started
    }

    fun announceQueued() {
        log.debug("Action ${data.name} of group ${data.groupId} and of type ${data.typeId} is Queued for execution")
        client.setData().forPath(actionPath, ActionStatus.Queued.value.toByteArray())
        data.status = ActionStatus.Queued
    }

    fun announceComplete() {
        log.debug("Action ${data.name} of group ${data.groupId} and of type ${data.typeId} Complete")
        client.setData().forPath(actionPath, ActionStatus.Complete.value.toByteArray())
        data.status = ActionStatus.Complete
    }

    fun announceCanceled() {
        log.debug("Action ${data.name} of group ${data.groupId} and of type ${data.typeId} was Canceled")
        client.setData().forPath(actionPath, ActionStatus.Canceled.value.toByteArray())
        data.status = ActionStatus.Canceled
    }

    protected fun announceFailure() {}

}
