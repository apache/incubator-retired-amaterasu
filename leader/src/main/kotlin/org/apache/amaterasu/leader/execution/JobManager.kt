package org.apache.amaterasu.leader.execution

import org.apache.amaterasu.common.configuration.enums.ActionStatus
import org.apache.amaterasu.common.dataobjects.ActionData
import org.apache.amaterasu.common.logging.KLogging
import org.apache.amaterasu.leader.common.execution.actions.Action
import org.apache.curator.framework.CuratorFramework
import java.util.concurrent.BlockingQueue

/**
 * Created by Eran Bartenstein on 10/11/18.
 */
class JobManager : KLogging() {
    var name: String = ""
    var jobId: String = ""
    lateinit var client: CuratorFramework
    lateinit var head: Action

    // TODO: this is not private due to tests, fix this!!!
    val registeredActions = HashMap<String, Action>()
    val frameworks = HashMap<String, HashSet<String>>()
    private lateinit var executionQueue: BlockingQueue<ActionData>

    /**
     * The start method initiates the job execution by executing the first action.
     * start mast be called once and by the JobManager only
     */
    fun start(): Unit = head.execute()

    val outOfActions: Boolean = registeredActions.filterValues {
        action -> action.data.status == ActionStatus.pending ||
            action.data.status == ActionStatus.queued ||
            action.data.status == ActionStatus.started
    }.isEmpty()

    /**
     * getNextActionData returns the data of the next action to be executed if such action
     * exists
     *
     * @return the ActionData of the next action, returns null if no such action exists
     */
    fun getNextActionData(): ActionData {

        val nextAction: ActionData = executionQueue.poll()

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
        registeredActions.put(action.actionId, action)
    }

    /**
     * announce the completion of an action and executes the next actions
     *
     * @param actionId
     */
    fun actionComplete(actionId: String) {

        val action = registeredActions[actionId]
        action!!.announceComplete()
        action.data.nextActionIds.forEach{id -> registeredActions[id]!!.execute()}

        // we don't need the error action anymore
        if (action.data.errorActionId != null)
            registeredActions[action.data.errorActionId]!!.announceCanceled()
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
        if (id != null)
            registeredActions[id]?.execute()

        //delete all future actions
        cancelFutureActions(action)
    }

    fun cancelFutureActions(action: Action) {

        if (action.data.status != ActionStatus.failed)
            action.announceCanceled()

        action.data.nextActionIds.forEach{id ->
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

}
