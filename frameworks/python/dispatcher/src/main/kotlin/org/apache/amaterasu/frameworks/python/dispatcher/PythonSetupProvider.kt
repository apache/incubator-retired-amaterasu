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
package org.apache.amaterasu.frameworks.python.dispatcher

import org.apache.amaterasu.common.configuration.ClusterConfig
import org.apache.amaterasu.frameworks.python.dispatcher.runners.providers.BasicPythonRunnerProvider
import org.apache.amaterasu.frameworks.python.dispatcher.runners.providers.PandasRunnerProvider
import org.apache.amaterasu.common.configuration.ConfigManager
import org.apache.amaterasu.sdk.frameworks.FrameworkSetupProvider
import org.apache.amaterasu.sdk.frameworks.RunnerSetupProvider
import org.apache.amaterasu.sdk.frameworks.configuration.DriverConfiguration
import java.io.File

class PythonSetupProvider : FrameworkSetupProvider {
    private var env: String? = null
    private var conf: ClusterConfig? = null
    private var runnerProviders: Map<String, RunnerSetupProvider> = mapOf()

    override val groupIdentifier: String
        get() = "python"
    override val groupResources: List<File>
        get() = listOf()

    override fun getDriverConfiguration(configManager: ConfigManager): DriverConfiguration {
        return DriverConfiguration(conf!!.taskMem(), 1) //TODO: this should be configured on env level
    }
    override val environmentVariables: Map<String, String>
        get() = mapOf()
    override val configurationItems: List<String>
        get() = listOf()

    override fun init(env: String, conf: ClusterConfig) {
        this.env = env
        this.conf = conf
        runnerProviders = runnerProviders + ("python" to BasicPythonRunnerProvider(env, conf))
        runnerProviders = runnerProviders + ("pandas" to PandasRunnerProvider(env, conf))
    }
    override fun getRunnerProvider(runnerId: String): RunnerSetupProvider {
        return runnerProviders.getValue(runnerId)
    }
}

