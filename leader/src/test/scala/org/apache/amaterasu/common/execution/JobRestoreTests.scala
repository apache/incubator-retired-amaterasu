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
package org.apache.amaterasu.common.execution

import java.util.concurrent.LinkedBlockingQueue

import org.apache.amaterasu.common.configuration.enums.ActionStatus
import org.apache.amaterasu.common.dataobjects.ActionData
import org.apache.amaterasu.leader.common.execution.JobManager
import org.apache.amaterasu.leader.execution.JobLoader
import org.apache.curator.framework.{CuratorFramework, CuratorFrameworkFactory}
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.curator.test.TestingServer
import org.apache.zookeeper.CreateMode
import org.scalatest.{BeforeAndAfterEach, DoNotDiscover, FlatSpec, Matchers}

import scala.io.Source

class JobRestoreTests extends FlatSpec with Matchers with BeforeAndAfterEach {

  val retryPolicy = new ExponentialBackoffRetry(1000, 3)
  val server = new TestingServer(2184, true)
  var client: CuratorFramework = null

  val jobId = s"job_${System.currentTimeMillis}"
  val maki = Source.fromURL(getClass.getResource("/simple-maki.yml")).mkString
  val queue = new LinkedBlockingQueue[ActionData]()

  var manager: JobManager = null

  client = CuratorFrameworkFactory.newClient(server.getConnectString, retryPolicy)
  client.start()

  override def beforeEach(): Unit = {

    // creating the jobs znode and storing the source repo and branch
    client.create().withMode(CreateMode.PERSISTENT).forPath(s"/$jobId")
    client.create().withMode(CreateMode.PERSISTENT).forPath(s"/$jobId/src", "".getBytes)
    client.create().withMode(CreateMode.PERSISTENT).forPath(s"/$jobId/branch", "".getBytes)

    manager = JobLoader.createJobManager(maki, jobId, client, 3, queue)

  }

  override def afterEach(): Unit = {

    client.delete().deletingChildrenIfNeeded().forPath(s"/$jobId")

  }

  "a restored job" should "have all Queued actions in the executionQueue" in {

    // setting task-0000000002 as Queued
    client.setData().forPath(s"/${jobId}/task-0000000002", ActionStatus.Queued.toString.getBytes)

    JobLoader.restoreJobState(manager, jobId, client)

    queue.peek.getName should be("start")
  }

  "a restored job" should "have all Started actions in the executionQueue" in {

    // setting task-0000000002 as Queued
    client.setData().forPath(s"/${jobId}/task-0000000002", ActionStatus.Started.toString.getBytes)

    JobLoader.restoreJobState(manager, jobId, client)

    queue.peek.getName should be("start")
  }
}
