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

import org.apache.amaterasu.common.dataobjects.ActionData
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.curator.test.TestingServer
import org.apache.zookeeper.CreateMode
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.given
import org.jetbrains.spek.api.dsl.it
import org.jetbrains.spek.api.dsl.on
import java.util.concurrent.LinkedBlockingQueue
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class JobExecutionTests : Spek({
    val retryPolicy = ExponentialBackoffRetry(1000, 3)
    val server = TestingServer(2183, true)
    val client = CuratorFrameworkFactory.newClient(server.connectString, retryPolicy)
    client.start()

    val jobId = "job_${System.currentTimeMillis()}"

    val yaml = this::class.java.getResource("/simple-maki.yml").readText()
    val queue = LinkedBlockingQueue<ActionData>()
    client.create().withMode(CreateMode.PERSISTENT).forPath("/$jobId")

    given("a job parsed in to a JobManager") {
        val job = JobParser.parse(jobId, yaml, queue, client, 1)

        it("queue the first action when the JobManager.start method is called ") {
            job.start()

            assertEquals(queue.peek().name, "start")

            // making sure that the status is reflected in zk
            val actionStatus = client.data.forPath("/$jobId/task-0000000000")
            assertEquals(String(actionStatus), "Queued")
        }

        it("return the start action when calling getNextAction and dequeue it") {
            assertEquals(job.nextActionData!!.name, "start")
            assertEquals(queue.size, 0)
        }

        it("also changes the status of start to started") {
            val actionStatus = client.data.forPath("/$jobId/task-0000000000")
            assertEquals(String(actionStatus), "Started")
        }

        it("is not out of actions when an action is still Pending") {
            assertFalse { job.outOfActions }
        }

        it("will requeue a task when calling JobManager.requeueAction") {
            job.requeueAction("0000000000")
            val actionStatus = client.data.forPath("/$jobId/task-0000000000")
            assertEquals(String(actionStatus), "Queued")
        }

        it("will restart the task") {
            val data = job.nextActionData
            assertEquals(data!!.name, "start")

            // making sure that the status is reflected in zk
            val actionStatus = client.data.forPath("/$jobId/task-0000000000")
            assertEquals(String(actionStatus), "Started")
        }

        it("will mark the action as Complete when the actionComplete method is called") {
            job.actionComplete("0000000000")
            // making sure that the status is reflected in zk
            val actionStatus = client.data.forPath("/$jobId/task-0000000000")

            assertEquals(String(actionStatus), "Complete")
        }

        on("completion of start, the next action step2 is queued") {
            assertEquals(queue.peek().name, "step2")
        }

        it("should also be registered as queued in zk") {
            val actionStatus = client.data.forPath("/$jobId/task-0000000001")
            assertEquals(String(actionStatus), "Queued")
        }

        it("is marked as Started when JobManager.nextActionData is called") {
            val data = job.nextActionData
            assertEquals(data!!.name, "step2")
        }

        it("will start an error action JobManager.actionFailed is called") {
            job.actionFailed("0000000001", "test failure")

            assertEquals(queue.peek().name, "error-action")
        }

        it("marks the error action as Complete when the actionComplete method is called"){
            job.actionComplete("0000000001-error")

            // making sure that the status is reflected in zk
            val actionStatus = client.data.forPath("/$jobId/task-0000000001-error")
            assertEquals(String(actionStatus) ,"Complete")
        }

        it("will be out of actions when all actions have been executed"){
            assertTrue { job.outOfActions }
        }
    }

})