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
import org.apache.amaterasu.common.dataobjects.ExecData
import org.apache.amaterasu.leader.common.utilities.DataLoader
import org.apache.amaterasu.sdk.frameworks.RunnerSetupProvider
import java.io.File

abstract class PythonRunnerProviderBase(env: String?, conf:ClusterConfig?) : RunnerSetupProvider() {
    override val runnerResources: Array<String>
        get() = arrayOf("python_sdk.zip")

    override fun getCommand(jobId: String, actionData: ActionData, env: String, executorId: String, callbackAddress: String): String {
        var cmd = "pip install -r ama-requirements.txt"
        execData.pyDeps()?.filePaths()?.forEach {
            path -> cmd += " && pip install -r ${path.split('/').last()}"
        }
        return cmd
    }

    override fun getActionDependencies(jobId: String, actionData: ActionData): Array<String> {
        File("ama-requirements.txt").appendText("./python_sdk.zip\n")
        return try {
            val userRequirements = execData.pyDeps()?.filePaths()
            arrayOf("ama-requirements.txt") + userRequirements!!
        } catch (e: NullPointerException) {
            arrayOf("ama-requirements.txt")
        }

    }

    override val hasExecutor: Boolean
        get() = false

    private val execData: ExecData = DataLoader.getExecutorData(env, conf)
}