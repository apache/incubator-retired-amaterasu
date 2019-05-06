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
package org.apache.amaterasu.leader.yarn

import org.apache.amaterasu.common.logging.KLogging
import org.apache.amaterasu.common.utils.ActiveNotifier
import org.apache.commons.lang.exception.ExceptionUtils

import java.nio.ByteBuffer

import org.apache.hadoop.yarn.api.records.ContainerId
import org.apache.hadoop.yarn.api.records.ContainerStatus
import org.apache.hadoop.yarn.client.api.async.NMClientAsync



class YarnNMCallbackHandler(val notifier: ActiveNotifier) : KLogging() , NMClientAsync.CallbackHandler {

    override fun onStartContainerError(containerId: ContainerId, t: Throwable) {
        notifier.error("Error starting a container ${t.message!!}", ExceptionUtils.getStackTrace(t))
    }

    override fun onGetContainerStatusError(containerId: ContainerId, t: Throwable) {
        notifier.error("","Couldn't get status from container ${containerId.containerId}. message ${t.message}")
    }

    override fun onContainerStatusReceived(containerId: ContainerId, containerStatus: ContainerStatus) {
        notifier.info("Container ${containerId.containerId} has status of ${containerStatus.state}")
    }

    override fun onContainerStarted(containerId: ContainerId, allServiceResponse: Map<String, ByteBuffer>) {
        notifier.info("Container ${containerId.containerId} Started")
    }

    override fun onStopContainerError(containerId: ContainerId, t: Throwable) {
        notifier.error("Error running a container ${t.message!!}", ExceptionUtils.getStackTrace(t))
    }

    override fun onContainerStopped(containerId: ContainerId) {
        notifier.info("Container ${containerId.containerId} stopped")
    }

}