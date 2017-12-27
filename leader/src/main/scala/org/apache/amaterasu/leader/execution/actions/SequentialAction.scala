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
package org.apache.amaterasu.leader.execution.actions

import java.util.concurrent.BlockingQueue

import org.apache.amaterasu.common.configuration.enums.ActionStatus
import org.apache.amaterasu.common.dataobjects.ActionData
import org.apache.curator.framework.CuratorFramework
import org.apache.zookeeper.CreateMode

import scala.collection.mutable.ListBuffer

class SequentialAction extends Action {

  var jobId: String = _
  var jobsQueue: BlockingQueue[ActionData] = _
  var attempts: Int = 2
  var attempt: Int = 1

  def execute(): Unit = {

    try {

      announceQueued
      jobsQueue.add(data)

    }
    catch {

      //TODO: this will not invoke the error action
      case e: Exception => handleFailure(e.getMessage)

    }

  }

  override def handleFailure(message: String): String = {

    log.debug(s"Part ${data.name} of group ${data.groupId} and of type ${data.typeId} failed on attempt $attempt with message: $message")
    attempt += 1

    if (attempt <= attempts) {
      data.id
    }
    else {
      announceFailure()
      println(s"===> moving to err action ${data.errorActionId}")
      data.status = ActionStatus.failed
      data.errorActionId
    }

  }

}

object SequentialAction {

  def apply(name: String,
            src: String,
            groupId: String,
            typeId: String,
            exports: Map[String, String],
            jobId: String,
            queue: BlockingQueue[ActionData],
            zkClient: CuratorFramework,
            attempts: Int): SequentialAction = {

    val action = new SequentialAction()

    action.jobsQueue = queue

    // creating a znode for the action
    action.client = zkClient
    action.actionPath = action.client.create().withMode(CreateMode.PERSISTENT_SEQUENTIAL).forPath(s"/$jobId/task-", ActionStatus.pending.toString.getBytes())
    action.actionId = action.actionPath.substring(action.actionPath.indexOf("task-") + 5)

    action.attempts = attempts
    action.jobId = jobId
    action.data = ActionData(ActionStatus.pending, name, src, groupId, typeId, action.actionId, exports, new ListBuffer[String])
    action.jobsQueue = queue
    action.client = zkClient

    action
  }

}

object ErrorAction {

  def apply(name: String,
            src: String,
            parent: String,
            groupId: String,
            typeId: String,
            jobId: String,
            queue: BlockingQueue[ActionData],
            zkClient: CuratorFramework): SequentialAction = {

    val action = new SequentialAction()

    action.jobsQueue = queue

    // creating a znode for the action
    action.client = zkClient
    action.actionPath = action.client.create().withMode(CreateMode.PERSISTENT).forPath(s"/$jobId/task-$parent-error", ActionStatus.pending.toString.getBytes())
    action.actionId = action.actionPath.substring(action.actionPath.indexOf('-') + 1).replace("/", "-")

    action.jobId = jobId
    action.data = ActionData(ActionStatus.pending, name, src, groupId, typeId, action.actionId, Map.empty, new ListBuffer[String])
    action.jobsQueue = queue
    action.client = zkClient

    action

  }
}
