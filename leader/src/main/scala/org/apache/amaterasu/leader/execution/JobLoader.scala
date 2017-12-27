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
package org.apache.amaterasu.leader.execution

import java.util.concurrent.BlockingQueue

import org.apache.amaterasu.common.configuration.enums.ActionStatus
import org.apache.amaterasu.common.dataobjects.ActionData
import org.apache.amaterasu.common.logging.Logging
import org.apache.amaterasu.leader.dsl.{GitUtil, JobParser}
import org.apache.curator.framework.CuratorFramework
import org.apache.zookeeper.CreateMode

import scala.collection.JavaConverters._


object JobLoader extends Logging {

  def loadJob(src: String, branch: String, jobId: String, client: CuratorFramework, attempts: Int, actionsQueue: BlockingQueue[ActionData]): JobManager = {

    // creating the jobs znode and storing the source repo and branch
    client.create().withMode(CreateMode.PERSISTENT).forPath(s"/$jobId")
    client.create().withMode(CreateMode.PERSISTENT).forPath(s"/$jobId/repo", src.getBytes)
    client.create().withMode(CreateMode.PERSISTENT).forPath(s"/$jobId/branch", branch.getBytes)

    val maki: String = loadMaki(src, branch)

    val jobManager: JobManager = createJobManager(maki, jobId, client, attempts, actionsQueue)

    //jobManager.start()
    jobManager

  }

  def createJobManager(maki: String, jobId: String, client: CuratorFramework, attempts: Int, actionsQueue: BlockingQueue[ActionData]): JobManager = {

    val jobManager = JobParser.parse(
      jobId,
      maki,
      actionsQueue,
      client,
      attempts
    )
    jobManager
  }

  def loadMaki(src: String, branch: String): String = {

    // cloning the git repo
    log.debug(s"getting repo: $src, for branch $branch")
    GitUtil.cloneRepo(src, branch)

    // parsing the maki.yaml and creating a JobManager to
    // coordinate the workflow based on the file
    val maki = JobParser.loadMakiFile()
    maki
  }

  def reloadJob(jobId: String, client: CuratorFramework, attempts: Int, actionsQueue: BlockingQueue[ActionData]) = {

    //val jobState = client.getChildren.forPath(s"/$jobId")
    val src = new String(client.getData.forPath(s"/$jobId/repo"))
    val branch = new String(client.getData.forPath(s"/$jobId/branch"))

    val maki: String = loadMaki(src, branch)

    val jobManager: JobManager = createJobManager(maki, jobId, client, attempts, actionsQueue)
    restoreJobState(jobManager, jobId, client)

    jobManager.start()
    jobManager
  }

  def restoreJobState(jobManager: JobManager, jobId: String, client: CuratorFramework): Unit = {

    val tasks = client.getChildren.forPath(s"/$jobId").asScala.toSeq.filter(n => n.startsWith("task"))
    for (task <- tasks) {

      if (client.getData.forPath(s"/$jobId/$task").sameElements(ActionStatus.queued.toString.getBytes) ||
        client.getData.forPath(s"/$jobId/$task").sameElements(ActionStatus.started.toString.getBytes)) {

        jobManager.reQueueAction(task.substring(task.indexOf("task-") + 5))

      }

    }

  }

}