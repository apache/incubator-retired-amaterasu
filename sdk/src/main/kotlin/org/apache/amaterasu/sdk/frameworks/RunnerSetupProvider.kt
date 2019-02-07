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
package org.apache.amaterasu.sdk.frameworks

import org.apache.amaterasu.common.dataobjects.ActionData

abstract class RunnerSetupProvider {

    private val actionFiles = arrayOf("env.yaml", "runtime.yaml", "datastores.yaml")

    abstract val runnerResources: Array<String>

    abstract fun getCommand(jobId: String, actionData: ActionData, env: String, executorId: String, callbackAddress: String): String

    abstract fun getActionUserResources(jobId: String, actionData: ActionData): Array<String>

    fun getActionResources(jobId: String, actionData: ActionData): Array<String> =
            actionFiles.map { f -> "$jobId/${actionData.name}/$f" }.toTypedArray() +
                    getActionUserResources(jobId, actionData)

    abstract fun getActionDependencies(jobId: String, actionData: ActionData): Array<String>

    abstract val hasExecutor: Boolean
       get
}