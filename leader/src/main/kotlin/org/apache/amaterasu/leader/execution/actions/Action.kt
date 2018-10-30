package org.apache.amaterasu.leader.execution.actions


import org.apache.amaterasu.common.dataobjects.ActionData
import org.apache.amaterasu.common.configuration.enums.ActionStatus
import org.apache.amaterasu.common.logging.Logging
import org.apache.curator.framework.CuratorFramework

/**
 * Created by Eran Bartenstein on 19/10/18.
 */
abstract class Action() : Logging {
    lateinit var actionPath: String
    lateinit var actionId: String
    lateinit var client: CuratorFramework
    lateinit var data: ActionData
    abstract fun execute()
    abstract fun handleFailure(message: String) : String

    fun announceStart() {
        log().debug("Starting action ${data.name} of group ${data.groupId} and type ${data.typeId}")
        client.setData().forPath(actionPath, ActionStatus.started.value.toByteArray())
        data.status = ActionStatus.started
    }

    fun announceQueued() {
        log().debug("Action ${data.name} of group ${data.groupId} and of type ${data.typeId} is queued for execution")
        client.setData().forPath(actionPath, ActionStatus.queued.value.toByteArray())
        data.status = ActionStatus.queued
    }

    fun announceComplete() {
        log().debug("Action ${data.name} of group ${data.groupId} and of type ${data.typeId} complete")
        client.setData().forPath(actionPath, ActionStatus.complete.value.toByteArray())
        data.status = ActionStatus.complete
    }

    fun announceCanceled() {
        log().debug("Action ${data.name} of group ${data.groupId} and of type ${data.typeId} was canceled")
        client.setData().forPath(actionPath, ActionStatus.canceled.value.toByteArray())
        data.status = ActionStatus.canceled
    }

    protected fun announceFailure() {}

}
