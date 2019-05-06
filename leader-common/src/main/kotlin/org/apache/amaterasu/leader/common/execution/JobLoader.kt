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
package org.apache.amaterasu.leader.common.execution

import org.apache.amaterasu.common.configuration.enums.ActionStatus
import org.apache.amaterasu.common.dataobjects.ActionData
import org.apache.amaterasu.common.logging.KLogging
import org.apache.amaterasu.leader.common.dsl.GitUtil
import org.apache.amaterasu.leader.common.dsl.JobParser
import org.apache.curator.framework.CuratorFramework
import org.apache.zookeeper.CreateMode
import java.util.concurrent.BlockingQueue

object JobLoader : KLogging() {

    @JvmStatic
    fun loadJob(src: String, branch: String, jobId: String, userName: String, password: String, client: CuratorFramework, attempts: Int, actionsQueue: BlockingQueue<ActionData>): JobManager {

        // creating the jobs znode and storing the source repo and branch
        client.create().withMode(CreateMode.PERSISTENT).forPath("/$jobId")
        client.create().withMode(CreateMode.PERSISTENT).forPath("/$jobId/repo", src.toByteArray())
        client.create().withMode(CreateMode.PERSISTENT).forPath("/$jobId/branch", branch.toByteArray())

        val maki: String = loadMaki(src, branch, userName, password)

        return createJobManager(maki, jobId, client, attempts, actionsQueue)

    }

    fun createJobManager(maki: String, jobId: String, client: CuratorFramework, attempts: Int, actionsQueue: BlockingQueue<ActionData>): JobManager {

        return JobParser.parse(
                jobId,
                maki,
                actionsQueue,
                client,
                attempts
        )
    }

    fun loadMaki(src: String, branch: String, userName: String = "", password: String = ""): String {

        // cloning the git repo
        log.debug("getting repo: $src, for branch $branch")
        GitUtil.cloneRepo(src, branch, userName, password)

        // parsing the maki.yaml and creating a JobManager to
        // coordinate the workflow based on the file
        val maki = JobParser.loadMakiFile()
        return maki
    }

    @JvmStatic
    fun reloadJob(jobId: String, userName: String, password: String, client: CuratorFramework, attempts: Int, actionsQueue: BlockingQueue<ActionData>): JobManager {

        //val jobState = client.getChildren.forPath(s"/$jobId")
        val src = String(client.data.forPath("/$jobId/repo"))
        val branch = String(client.data.forPath("/$jobId/branch"))

        val maki: String = loadMaki(src, branch, userName, password)

        val jobManager: JobManager = createJobManager(maki, jobId, client, attempts, actionsQueue)
        restoreJobState(jobManager, jobId, client)

        jobManager.start()
        return jobManager
    }

    fun restoreJobState(jobManager: JobManager, jobId: String, client: CuratorFramework): Unit {

        val tasks = client.children.forPath("/$jobId").filter { it.startsWith("task") }

        for (task in tasks) {

            val status = ActionStatus.valueOf(String(client.data.forPath("/$jobId/$task")))
            if (status == ActionStatus.Queued || status == ActionStatus.Started) {
                jobManager.reQueueAction(task.substring(task.indexOf("task-") + 5))
            }

        }

    }


}