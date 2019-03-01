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
import org.apache.amaterasu.leader.common.execution.actions.Action
import org.apache.curator.framework.CuratorFramework
import java.util.concurrent.BlockingQueue

data class JobManager(var name: String = "",
                      var jobId: String = "",
                      var executionQueue: BlockingQueue<ActionData>,
                      var client: CuratorFramework) : KLogging() {

    override fun toString(): String {
        val result = StringBuilder()
        return result.toString()
    }

    lateinit var head: Action

    // TODO: this is not private due to tests, fix this!!!
    val registeredActions = HashMap<String, Action>()
    val frameworks = HashMap<String, HashSet<String>>()

    /**
     * The start method initiates the job execution by executing the first action.
     * start mast be called once and by the JobManager only
     */
    fun start(): Unit = head.execute()

    val outOfActions: Boolean
        get() {
            return !(registeredActions.values.map { it.data.status }.contains(ActionStatus.Pending)) &&
                    !(registeredActions.values.map { it.data.status }.contains(ActionStatus.Queued)) &&
                    !(registeredActions.values.map { it.data.status }.contains(ActionStatus.Started))
        }

    /**
     * getNextActionData returns the data of the next action to be executed if such action
     * exists
     *
     * @return the ActionData of the next action, returns null if no such action exists
     */
    val nextActionData: ActionData?
        get() {

            val nextAction: ActionData? = executionQueue.poll()

            if (nextAction != null) {
                registeredActions[nextAction.id]!!.announceStart()
            }

            return nextAction
        }

    fun reQueueAction(actionId: String) {

        val action = registeredActions[actionId]
        executionQueue.put(action!!.data)
        registeredActions[actionId]!!.announceQueued()

    }

    /**
     * Registers an action with the job
     *
     * @param action
     */
    fun registerAction(action: Action) {
        registeredActions[action.actionId] = action
    }

    /**
     * announce the completion of an action and executes the next actions
     *
     * @param actionId
     */
    fun actionComplete(actionId: String) {
        val action = registeredActions[actionId]
        action?.let {

            it.announceComplete()

            action.data.nextActionIds.forEach { id -> registeredActions[id]!!.execute() }

            // we don't need the error action anymore
            if (it.data.hasErrorAction)
                registeredActions[action.data.errorActionId]!!.announceCanceled()
        }

    }

    /**
     * gets the next action id which can be either the same action or an error action
     * and if it exist (we have an error action or a retry)
     *
     * @param actionId
     */
    fun actionFailed(actionId: String, message: String) {

        log.warn(message)

        val action = registeredActions[actionId]
        val id = action!!.handleFailure(message)
        if (!id.isEmpty())
            registeredActions[id]?.execute()

        //delete all future actions
        cancelFutureActions(action)
    }

    fun cancelFutureActions(action: Action) {

        if (action.data.status != ActionStatus.Failed)
            action.announceCanceled()

        action.data.nextActionIds.forEach { id ->
            val registeredAction = registeredActions[id]
            if (registeredAction != null) {
                cancelFutureActions(registeredAction)
            }
        }
    }

    /**
     * announce the start of execution of the action
     */
    fun actionStarted(actionId: String) {

        val action = registeredActions[actionId]
        action?.announceStart()

    }

    fun actionsCount(): Int = executionQueue.size

    val isInitialized: Boolean
        get() = ::head.isInitialized
}

