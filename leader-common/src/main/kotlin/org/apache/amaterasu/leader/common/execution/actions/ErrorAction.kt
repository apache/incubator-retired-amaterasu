package org.apache.amaterasu.leader.common.execution.actions

import org.apache.amaterasu.common.configuration.enums.ActionStatus
import org.apache.amaterasu.common.dataobjects.ActionData
import org.apache.curator.framework.CuratorFramework
import org.apache.zookeeper.CreateMode
import java.util.concurrent.BlockingQueue

class ErrorAction(name: String,
                  src: String,
                  parent: String,
                  groupId: String,
                  typeId: String,
                  jobId: String,
                  queue: BlockingQueue<ActionData>,
                  zkClient: CuratorFramework) : SequentialActionBase() {

    init {
        jobsQueue = queue

        // creating a znode for the action
        client = zkClient
        actionPath = client.create().withMode(CreateMode.PERSISTENT).forPath("/$jobId/task-$parent-error", ActionStatus.pending.toString().toByteArray())
        actionId = actionPath.substring(actionPath.indexOf('-') + 1).replace("/", "-")

        this.jobId = jobId
        data =  ActionData (ActionStatus.pending, name, src, groupId, typeId, actionId, hashMapOf(), arrayListOf())
        jobsQueue = queue
        client = zkClient
    }
}