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
import java.util.concurrent.BlockingQueue

open class SequentialActionBase : Action() {


    var jobId: String = ""
    lateinit var jobsQueue: BlockingQueue<ActionData>
    var attempts: Int = 2
    private var attempt: Int = 1

    override fun execute() {

        try {

            announceQueued()
            jobsQueue.add(data)

        }
        catch(e: Exception) {

            //TODO: this will not invoke the error action
            e.message?.let{ handleFailure(it) }

        }

    }

    override fun handleFailure(message: String): String {

        println("Part ${data.name} of group ${data.groupId} and of type ${data.typeId} Failed on attempt $attempt with message: $message")
        attempt += 1

        var result = ""
        if (attempt <= attempts) {
            result = data.id
        }
        else {
            announceFailure()
            if(data.hasErrorAction) {
                println("===> moving to err action ${data.errorActionId}")
                data.status = ActionStatus.Failed
                result = data.errorActionId
            }
        }
        return result
    }

}
