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

import org.apache.amaterasu.common.configuration.enums.ActionStatus
import org.apache.amaterasu.common.dataobjects.ActionData
import org.apache.curator.framework.CuratorFramework
import org.apache.zookeeper.CreateMode
import java.util.concurrent.BlockingQueue

class ErrorAction(name: String,
                  src: String,
                  parent: String,
                  config: String,
                  groupId: String,
                  typeId: String,
                  jobId: String,
                  queue: BlockingQueue<ActionData>,
                  zkClient: CuratorFramework) : SequentialActionBase() {

    init {
        jobsQueue = queue

        // creating a znode for the action
        client = zkClient
        actionPath = client.create().withMode(CreateMode.PERSISTENT).forPath("/$jobId/task-$parent-error", ActionStatus.Pending.toString().toByteArray())
        actionId = actionPath.substring(actionPath.indexOf('-') + 1).replace("/", "-")

        this.jobId = jobId
        data = ActionData(ActionStatus.Pending, name, src, config, groupId, typeId, actionId)
        jobsQueue = queue
        client = zkClient
    }
}