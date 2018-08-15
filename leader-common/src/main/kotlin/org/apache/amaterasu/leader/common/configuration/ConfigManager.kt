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
package org.apache.amaterasu.leader.common.configuration

import com.uchuhimo.konf.Config
import com.uchuhimo.konf.source.yaml.toYaml
import java.io.File

class ConfigManager(private val env: String, private val repoPath: String, private val frameworkItems: List<String> = emptyList()) {

    private val envFolder = "$repoPath/env/$env"

    // this is currently public for testing reasons, need to reconsider
    var config: Config = Config {
        addSpec(Job)
        for (item in frameworkItems) {
            val frameworkSpec = GenericSpec(item)
            addSpec(frameworkSpec.spec)
        }
    }

    init {
        for (file in File(envFolder).listFiles()) {
            config = config.from.yaml.file(file)
            println(config.toYaml.toText())
        }
    }

    fun getActionConfiguration(action: String, path: String = ""): Config {

        val actionPath = if (path.isEmpty()) {
            "$repoPath/src/$action/env/$env"
        } else {
            "$repoPath/$path"
                    .replace("{env}", env)
                    .replace("{action_name}", action)
        }

        var result = config

        val configLocation = File(actionPath)
        if (configLocation.exists()) {
            if (configLocation.isDirectory) {
                for (file in File(actionPath).listFiles()) {
                    result = config.from.yaml.file(file)
                }
            } else {
                result = config.from.yaml.file(configLocation)
            }
        }
        return result
    }
}