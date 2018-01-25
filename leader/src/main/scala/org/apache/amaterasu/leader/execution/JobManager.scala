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
import org.apache.amaterasu.leader.execution.actions.Action
import org.apache.curator.framework.CuratorFramework

import scala.collection.concurrent.TrieMap
import scala.collection.mutable

/**
  * The JobManager manages the lifecycle of a job. It queues new actions for execution,
  * tracks the state of actions and is in charge of communication with the underlying
  * cluster management framework (mesos)
  */
class JobManager extends Logging {

  var name: String = _
  var jobId: String = _
  var client: CuratorFramework = _
  var head: Action = _

  // TODO: this is not private due to tests, fix this!!!
  val registeredActions = new TrieMap[String, Action]
  val frameworks = new TrieMap[String, mutable.HashSet[String]]
  private var executionQueue: BlockingQueue[ActionData] = _

  /**
    * The start method initiates the job execution by executing the first action.
    * start mast be called once and by the JobManager only
    */
  def start(): Unit = {

    head.execute()

  }

  def outOfActions: Boolean = !registeredActions.values.exists(a => a.data.status == ActionStatus.pending ||
    a.data.status == ActionStatus.queued ||
    a.data.status == ActionStatus.started)
  /**
    * getNextActionData returns the data of the next action to be executed if such action
    * exists
    *
    * @return the ActionData of the next action, returns null if no such action exists
    */
  def getNextActionData: ActionData = {

    val nextAction: ActionData = executionQueue.poll()

    if (nextAction != null) {
      registeredActions(nextAction.id).announceStart
    }

    nextAction
  }

  def reQueueAction(actionId: String): Unit = {

    val action = registeredActions(actionId)
    executionQueue.put(action.data)
    registeredActions(actionId).announceQueued

  }

  /**
    * Registers an action with the job
    *
    * @param action
    */
  def registerAction(action: Action): Unit = {

    registeredActions.put(action.actionId, action)

  }

  /**
    * announce the completion of an action and executes the next actions
    *
    * @param actionId
    */
  def actionComplete(actionId: String): Unit = {

    val action = registeredActions.get(actionId).get
    action.announceComplete
    action.data.nextActionIds.foreach(id =>
      registeredActions.get(id).get.execute())

    // we don't need the error action anymore
    if (action.data.errorActionId != null)
      registeredActions.get(action.data.errorActionId).get.announceCanceled
  }

  /**
    * gets the next action id which can be either the same action or an error action
    * and if it exist (we have an error action or a retry)
    *
    * @param actionId
    */
  def actionFailed(actionId: String, message: String): Unit = {

    log.warn(message)

    val action = registeredActions.get(actionId).get
    val id = action.handleFailure(message)
    if (id != null)
      registeredActions.get(id).get.execute()

    //delete all future actions
    cancelFutureActions(action)
  }

  def cancelFutureActions(action: Action): Unit = {

    if (action.data.status != ActionStatus.failed)
      action.announceCanceled

    action.data.nextActionIds.foreach(id =>
      cancelFutureActions(registeredActions.get(id).get))
  }

  /**
    * announce the start of execution of the action
    */
  def actionStarted(actionId: String): Unit = {

    val action = registeredActions.get(actionId).get
    action.announceStart

  }

  def actionsCount(): Int = {
    executionQueue.size()
  }
}

object JobManager {

  /**
    * The apply method starts the job execution once the job is created from the maki.yaml file
    * it is in charge of creating the internal flow map, setting up ZooKeeper and executing
    * the first action
    * If the job execution is resumed (a job that was stooped) the init method will restore the
    * state of the job from ZooKepper
    *
    * @param jobId
    * @param name
    * @param jobsQueue
    * @param client
    * @return
    */
  def apply(
    jobId: String,
    name: String,
    jobsQueue: BlockingQueue[ActionData],
    client: CuratorFramework
  ): JobManager = {

    val manager = new JobManager()
    manager.name = name
    manager.executionQueue = jobsQueue
    manager.jobId = jobId
    manager.client = client

    manager

  }

}