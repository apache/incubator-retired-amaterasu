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
import java.util.concurrent.LinkedBlockingQueue
import kotlin.test.assertEquals

class JobParserTests : Spek({

    val retryPolicy = ExponentialBackoffRetry(1000, 3)
    val server = TestingServer(2187, true)
    val client = CuratorFrameworkFactory.newClient(server.connectString, retryPolicy)
    client.start()

    val jobId = "job_${System.currentTimeMillis()}"
    val queue = LinkedBlockingQueue<ActionData>()

    // this will be performed by the job bootstrapper
    client.create().withMode(CreateMode.PERSISTENT).forPath("/$jobId")

    given("a simple-maki.yaml that is passed to a JobParser") {

        val yaml = this::class.java.getResource("/simple-maki.yml").readText()
        val job = JobParser.parse(jobId, yaml, queue, client, 1)

        it("parse the job details correctly") {
            assertEquals(job.name, "amaterasu-test")
        }

        it("also have two actions in the right order") {

            assertEquals(job.registeredActions.size, 3)

            assertEquals(job.registeredActions["0000000000"]!!.data.name, "start")
            assertEquals(job.registeredActions["0000000001"]!!.data.name, "step2")
            assertEquals(job.registeredActions["0000000001-error"]!!.data.name, "error-action")

        }

        it("also also parse action 'config' successfully") {
            assertEquals(job.registeredActions["0000000000"]!!.data.config, "start-cfg.yaml")
            assertEquals(job.registeredActions["0000000001"]!!.data.config, "step2-cfg.yaml")
            assertEquals(job.registeredActions["0000000001-error"]!!.data.config, "error-cfg.yaml")
        }
    }
})