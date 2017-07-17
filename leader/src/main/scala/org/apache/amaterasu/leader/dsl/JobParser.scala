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
package org.apache.amaterasu.leader.dsl

import java.util.concurrent.BlockingQueue

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import org.apache.amaterasu.common.dataobjects.ActionData
import org.apache.amaterasu.leader.execution.actions.{Action, ErrorAction, SequentialAction}
import org.apache.amaterasu.leader.execution.JobManager
import org.apache.curator.framework.CuratorFramework

import scala.collection.JavaConverters._
import scala.io.Source

/**
  * The JobParser class is in charge of parsing the maki.yaml file which
  * describes the workflow of an amaterasu job
  */
object JobParser {

  def loadMakiFile(): String = {

    Source.fromFile("repo/maki.yml").mkString

  }

  /**
    * Parses the maki.yml string and creates a job manager
    *
    * @param jobId
    * @param maki a string containing the YAML definition of the job
    * @param actionsQueue
    * @param client
    * @return
    */
  def parse(jobId: String,
            maki: String,
            actionsQueue: BlockingQueue[ActionData],
            client: CuratorFramework,
            attempts: Int): JobManager = {

    val mapper = new ObjectMapper(new YAMLFactory())

    val job = mapper.readTree(maki)

    // loading the job details
    val manager = JobManager(jobId, job.path("job-name").asText, actionsQueue, client)

    // iterating the flow list and constructing the job's flow
    val actions = job.path("flow").asInstanceOf[ArrayNode].asScala.toSeq

    parseActions(actions, manager, actionsQueue, attempts, null)

    manager
  }

  /**
    * parseActions is a recursive function, for building the workflow of
    * the job
    * God, I miss Clojure
    *
    * @param actions  a seq containing the definitions of all the actions
    * @param manager  the job manager for the job
    * @param actionsQueue
    * @param previous the previous action, this is used in order to add the current action
    *                 to the nextActionIds
    */
  def parseActions(
    actions: Seq[JsonNode],
    manager: JobManager,
    actionsQueue: BlockingQueue[ActionData],
    attempts: Int,
    previous: Action
  ): Unit = {

    if (actions.isEmpty)
      return

    val actionData = actions.head

    val action = parseSequentialAction(
      actionData,
      manager.jobId,
      actionsQueue,
      manager.client,
      attempts
    )

    if (manager.head == null)
      manager.head = action

    if (previous != null)
      previous.data.nextActionIds.append(action.actionId)

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
    }

    parseActions(actions.tail, manager, actionsQueue, attempts, action)

  }

  def parseSequentialAction(
    action: JsonNode,
    jobId: String,
    actionsQueue: BlockingQueue[ActionData],
    client: CuratorFramework,
    attempts: Int
  ): SequentialAction = {

    SequentialAction(
      action.path("name").asText,
      action.path("file").asText,
      action.path("runner").path("group").asText,
      action.path("runner").path("type").asText,
      action.path("exports").fields().asScala.toSeq.map(e=> (e.getKey, e.getValue.asText())).toMap,
      jobId,
      actionsQueue,
      client,
      attempts
    )

  }

  def parseErrorAction(
    action: JsonNode,
    jobId: String,
    parent: String,
    actionsQueue: BlockingQueue[ActionData],
    client: CuratorFramework
  ): SequentialAction = {

    ErrorAction(
      action.path("name").asText,
      action.path("file").asText,
      parent,
      action.path("group").asText,
      action.path("type").asText,
      jobId,
      actionsQueue,
      client
    )

  }
}
