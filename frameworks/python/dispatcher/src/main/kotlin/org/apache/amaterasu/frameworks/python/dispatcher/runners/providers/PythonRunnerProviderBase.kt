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

import com.uchuhimo.konf.Config
import org.apache.amaterasu.common.configuration.ClusterConfig
import org.apache.amaterasu.common.dataobjects.ActionData
import org.apache.amaterasu.leader.common.utilities.DataLoader
import org.apache.amaterasu.sdk.frameworks.RunnerSetupProvider
import java.io.File
import java.nio.file.Files
import java.nio.file.Paths

abstract class PythonRunnerProviderBase(val env: String, val conf: ClusterConfig) : RunnerSetupProvider() {

    private val requirementsFileName: String = "ama-requirements.txt"
    private val mandatoryPYPIPackages: Array<String> = arrayOf("requests")
    protected val virtualPythonPath = "amaterasu_env/bin/python"
    protected val virtualPythonBin = "amaterasu_env/bin"

    override val runnerResources: Array<String>
        get() = arrayOf("amaterasu-sdk-${conf.version()}.zip")

    override fun getCommand(jobId: String, actionData: ActionData, env: Config, executorId: String, callbackAddress: String): String {
        val pythonPath = conf.pythonPath()
        val virtualEnvCmd = "$pythonPath -m venv amaterasu_env"
        val installBaseRequirementsCmd = "$virtualPythonPath -m pip install --upgrade --force-reinstall -r $requirementsFileName"
        var cmd = "$virtualEnvCmd && $installBaseRequirementsCmd"
        val execData = DataLoader.getExecutorData(this.env, conf)
        execData.pyDeps?.filePaths?.forEach {
            path -> cmd += " && $pythonPath -m pip install -r ${path.split('/').last()}"
        }
        return cmd
    }

    override fun getActionDependencies(jobId: String, actionData: ActionData): Array<String> {
        val reqFile = File(requirementsFileName)

        if (reqFile.exists()) reqFile.delete()

        val dependencies = runnerResources + mandatoryPYPIPackages

        dependencies.forEach { resource ->
            println("====> RESOURCES: $resource")
            reqFile.appendText("$resource\n")
        }

        return try {
            val execData = DataLoader.getExecutorData(env, conf)
            val userRequirements = execData.pyDeps?.filePaths
            arrayOf(reqFile.name) + userRequirements!!
        } catch (e: NullPointerException) {
            arrayOf(reqFile.name)
        }

    }

    override val hasExecutor: Boolean
        get() = false


}