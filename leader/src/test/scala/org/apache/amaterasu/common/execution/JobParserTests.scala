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

import org.apache.amaterasu.common.dataobjects.ActionData
import org.apache.amaterasu.leader.common.dsl.JobParser
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.curator.test.TestingServer
import org.apache.zookeeper.CreateMode
import org.scalatest.{FlatSpec, Matchers}

import scala.io.Source

class JobParserTests extends FlatSpec with Matchers {

  private val retryPolicy = new ExponentialBackoffRetry(1000, 3)
  private val server = new TestingServer(2182, true)
  private val client = CuratorFrameworkFactory.newClient(server.getConnectString, retryPolicy)
  client.start()

  private val jobId = s"job_${System.currentTimeMillis}"
  private val yaml = Source.fromURL(getClass.getResource("/simple-maki.yml")).mkString
  private val queue = new LinkedBlockingQueue[ActionData]()

  // this will be performed by the job bootstrapper
  client.create().withMode(CreateMode.PERSISTENT).forPath(s"/$jobId")

  private val job = JobParser.parse(jobId, yaml, queue, client, 1)

  "JobParser" should "parse the simple-maki.yml" in {

    job.getName should be("amaterasu-test")

  }

  //TODO: I suspect this test is not indicative, and that order is assured need to verify this
  it should "also have two actions in the right order" in {

    job.getRegisteredActions.size should be(3)

    job.getRegisteredActions.get("0000000000").data.getName should be("start")
    job.getRegisteredActions.get("0000000001").data.getName should be("step2")
    job.getRegisteredActions.get("0000000001-error").data.getName should be("error-action")

  }

  it should "Action 'config' is parsed successfully" in {

    job.getRegisteredActions.size should be(3)

    job.getRegisteredActions.get("0000000000").data.getConfig should be("start-cfg.yaml")
    job.getRegisteredActions.get("0000000001").data.getConfig should be("step2-cfg.yaml")
    job.getRegisteredActions.get("0000000001-error").data.getConfig should be("error-cfg.yaml")

  }

}
