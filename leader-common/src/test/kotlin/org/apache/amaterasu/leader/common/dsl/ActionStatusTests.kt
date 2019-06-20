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
package org.apache.amaterasu.leader.common.dsl

import org.apache.amaterasu.common.configuration.enums.ActionStatus
import org.apache.amaterasu.common.dataobjects.ActionData
import org.apache.amaterasu.leader.common.execution.actions.SequentialAction
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.curator.test.TestingServer
import org.apache.zookeeper.CreateMode
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.given
import org.jetbrains.spek.api.dsl.it
import java.util.concurrent.LinkedBlockingQueue
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

class ActionStatusTests : Spek({

    // setting up a testing zookeeper server (curator TestServer)
    val retryPolicy = ExponentialBackoffRetry(1000, 3)
    val server = TestingServer(2181, true)
    val jobId = "job_${System.currentTimeMillis()}"


    given("an action") {

        val data = ActionData(ActionStatus.Pending, "test_action", "start.scala", "", "spark", "scala", "0000001", hashMapOf(), mutableListOf())
        val queue = LinkedBlockingQueue<ActionData>()

        val client = CuratorFrameworkFactory.newClient(server.connectString, retryPolicy)
        client.start()

        client.create().withMode(CreateMode.PERSISTENT).forPath("/$jobId")
        val action = SequentialAction(data.name, data.src, "", data.groupId, data.typeId, mapOf(), jobId, queue, client, 1)

        it("should queue it's ActionData int the job queue when executed") {
            action.execute()
            assertEquals(queue.peek().name, data.name)
            assertEquals(queue.peek().src, data.src)
        }

        it("should also create a sequential znode for the task with the value of Queued") {
            val taskStatus = client.data.forPath("/$jobId/task-0000000000")

            assertNotNull(taskStatus)
            assertEquals(String(taskStatus), "Queued")

        }
    }
})