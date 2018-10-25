package org.apache.amaterasu.leader.execution


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
        val startedAction: String = ActionStatus.started.value
        client.setData().forPath(actionPath, startedAction.toByteArray())
        data.status = ActionStatus.started
    }
}