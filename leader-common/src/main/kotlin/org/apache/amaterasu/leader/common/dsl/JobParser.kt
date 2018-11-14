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

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import org.apache.amaterasu.common.dataobjects.ActionData
import org.apache.amaterasu.leader.common.execution.actions.Action
import org.apache.amaterasu.leader.common.execution.actions.ErrorAction
import org.apache.amaterasu.leader.common.execution.actions.SequentialAction
import org.apache.amaterasu.leader.common.execution.JobManager
import org.apache.curator.framework.CuratorFramework
import java.io.File
import java.util.concurrent.BlockingQueue

class JobParser {
    fun loadMakiFile(): String = File("repo/maki.yml").readText(Charsets.UTF_8)

    /**
     * Parses the maki.yml string and creates a job manager
     *
     * @param jobId
     * @param maki a string containing the YAML definition of the job
     * @param actionsQueue
     * @param client
     * @return
     */
    fun parse(jobId: String,
              maki: String,
              actionsQueue: BlockingQueue<ActionData>,
              client: CuratorFramework,
              attempts: Int): JobManager {

        val mapper = ObjectMapper(YAMLFactory())

        val job = mapper.readTree(maki)

        // loading the job details
        val manager = JobManager(jobId, job.path("job-name").asText(), actionsQueue, client)

        // iterating the flow list and constructing the job's flow
        val actions = (job.path("flow") as ArrayNode).toList()

        parseActions(actions, manager, actionsQueue, attempts, null)

        return manager
    }

    fun parseActions(actions: List<JsonNode>,
                     manager: JobManager,
                     actionsQueue: BlockingQueue<ActionData>,
                     attempts: Int,
                     previous: Action?) {

        if (actions.isEmpty())
            return

        val actionData = actions.first()

        val action = parseSequentialAction(
                actionData,
                manager.jobId,
                actionsQueue,
                manager.client,
                attempts
        )

        //updating the list of frameworks setup
        manager.frameworks.getOrPut(action.data.groupId){HashSet()}
                .add(action.data.typeId)


        if (manager.head == null) {
            manager.head = action
        }

        if (previous != null) {
            ArrayList(previous.data.nextActionIds).add(action.actionId)
        }
        manager.registerAction(action)

        val errorNode = actionData.path("error")

        if (!errorNode.isMissingNode) {

            val errorAction = parseErrorAction(
                    errorNode,
                    manager.jobId,
                    action.data.id,
                    actionsQueue,
                    manager.client
            )

            action.data.errorActionId = errorAction.data.id
            manager.registerAction(errorAction)

            //updating the list of frameworks setup
            manager.frameworks.getOrPut(errorAction.data.groupId){HashSet()}
                    .add(errorAction.data.typeId)
        }

        parseActions(actions.drop(1), manager, actionsQueue, attempts, action)

    }

    fun parseSequentialAction(action: JsonNode,
    jobId: String,
    actionsQueue: BlockingQueue<ActionData>,
    client: CuratorFramework,
    attempts: Int): SequentialAction {

        return  SequentialAction(action.path("name").asText(),
                action.path("file").asText(),
                action.path("runner").path("group").asText(),
                action.path("runner").path("type").asText(),
                action.path("exports").fields().asSequence().map { it.key to it.value.asText() }.toMap(),
        jobId,
        actionsQueue,
        client,
        attempts)

    }

    fun parseErrorAction(action: JsonNode,
                         jobId: String,
                         parent: String,
                         actionsQueue: BlockingQueue<ActionData>,
                         client: CuratorFramework): ErrorAction {

        return ErrorAction(
                action.path("name").asText(),
                action.path("file").asText(),
                parent,
                action.path("group").asText(),
                action.path("type").asText(),
                jobId,
                actionsQueue,
                client
        )

    }

}