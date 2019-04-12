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
package org.apache.amaterasu.frameworks.python.dispatcher.runners.providers

import org.apache.amaterasu.common.configuration.ClusterConfig
import org.apache.amaterasu.common.dataobjects.ActionData

 class BasicPythonRunnerProvider(env: String, conf: ClusterConfig) : PythonRunnerProviderBase(env, conf) {
    override val runnerResources: Array<String>
        get() {
            var resources = super.runnerResources
            resources += "amaterasu_python-${conf.version()}.zip"
            return resources
        }

    override fun getActionUserResources(jobId: String, actionData: ActionData): Array<String> = arrayOf()

    override fun getCommand(jobId: String, actionData: ActionData, env: String, executorId: String, callbackAddress: String): String {
        return super.getCommand(jobId, actionData, env, executorId, callbackAddress) + " && $virtualPythonPath ${actionData.src}"
    }

}