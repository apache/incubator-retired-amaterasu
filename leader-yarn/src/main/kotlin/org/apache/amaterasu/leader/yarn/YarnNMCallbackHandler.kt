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

import java.nio.ByteBuffer

import org.apache.hadoop.yarn.api.records.ContainerId
import org.apache.hadoop.yarn.api.records.ContainerStatus
import org.apache.hadoop.yarn.client.api.async.NMClientAsync



class YarnNMCallbackHandler : KLogging() , NMClientAsync.CallbackHandler {

    override fun onStartContainerError(containerId: ContainerId, t: Throwable) {
        log.error("Container ${containerId.containerId} couldn't start.", t)
    }

    override fun onGetContainerStatusError(containerId: ContainerId, t: Throwable) {
        log.error("Couldn't get status from container ${containerId.containerId}.", t)
    }

    override fun onContainerStatusReceived(containerId: ContainerId, containerStatus: ContainerStatus) {
        log.info("Container ${containerId.containerId} has status of ${containerStatus.state}")
    }

    override fun onContainerStarted(containerId: ContainerId, allServiceResponse: Map<String, ByteBuffer>) {
        log.info("Container ${containerId.containerId} Started")
    }

    override fun onStopContainerError(containerId: ContainerId, t: Throwable) {
        log.error("Container ${containerId.containerId} has thrown an error", t)
    }

    override fun onContainerStopped(containerId: ContainerId) {
        log.info("Container ${containerId.containerId} stopped")
    }

}