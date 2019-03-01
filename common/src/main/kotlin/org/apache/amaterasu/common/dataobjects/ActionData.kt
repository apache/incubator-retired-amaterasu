/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.amaterasu.common.dataobjects

import org.apache.amaterasu.common.configuration.enums.ActionStatus

/*
    Adding default values just for the sake of Scala
 */
data class ActionData(var status: ActionStatus = ActionStatus.Pending,
                      var name: String= "",
                      var src: String= "",
                      var config: String= "",
                      var groupId: String= "",
                      var typeId: String= "",
                      var id: String= "",
                      var exports: Map<String, String> = mutableMapOf(),
                      var nextActionIds: MutableList<String> = mutableListOf()) {

    lateinit var errorActionId: String
    lateinit var artifact: Artifact
    lateinit var repo: Repo
    lateinit var entryClass: String

    val hasErrorAction: Boolean
        get() = ::errorActionId.isInitialized

    val hasArtifact: Boolean
        get() = ::artifact.isInitialized
}
