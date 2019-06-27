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
package org.apache.amaterasu.leader.common.utilities

import java.io.File
import java.io.FileInputStream
import java.nio.file.Files
import java.nio.file.Paths

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.kotlin.KotlinModule
import com.fasterxml.jackson.module.kotlin.readValue
import org.apache.amaterasu.leader.common.configuration.ClusterConfig
import org.apache.amaterasu.common.dataobjects.TaskData
import org.apache.amaterasu.common.dataobjects.ActionData
import org.apache.amaterasu.common.dataobjects.ExecData

import org.apache.amaterasu.common.execution.dependencies.Dependencies
import org.apache.amaterasu.common.execution.dependencies.PythonDependencies
import org.apache.amaterasu.common.logging.KLogging

import org.apache.amaterasu.common.runtime.Environment
import org.yaml.snakeyaml.Yaml


object DataLoader : KLogging() {

    private val mapper = ObjectMapper()

    private val ymlMapper = ObjectMapper(YAMLFactory())

    init {
        mapper.registerModule(KotlinModule())
        ymlMapper.registerModule(KotlinModule())
    }

    @JvmStatic
    fun getTaskDataBytes(actionData: ActionData, env: String): ByteArray {
        return mapper.writeValueAsBytes(getTaskData(actionData, env))
    }

    @JvmStatic
    fun getTaskData(actionData: ActionData, env: String): TaskData {
        val srcFile = actionData.src
        var src = ""

        if (srcFile.isNotEmpty()) {
            src = File("repo/src/$srcFile").readText()
        }

        val envValue = File("repo/env/$env/job.yml").readText()

        val envData = ymlMapper.readValue<Environment>(envValue)

        val exports = actionData.exports

        return TaskData(src, envData, actionData.groupId, actionData.typeId, exports)
    }

    @JvmStatic
    fun getDatasets(env: String): String {
        var file = File("repo/env/$env/datasets.yml")
        return if (file.exists()) {
            file.readText()
        } else {
            file = File("repo/env/$env/datasets.yaml")
            if (file.exists()) {
                file.readText()
            } else {
                ""
            }
        }
    }

    @JvmStatic
    fun getTaskDataString(actionData: ActionData, env: String): String {
        return mapper.writeValueAsString(getTaskData(actionData, env))
    }

    @JvmStatic
    fun getExecutorDataBytes(env: String, clusterConf: ClusterConfig): ByteArray {
        return mapper.writeValueAsBytes(getExecutorData(env, clusterConf))
    }

    @JvmStatic
    fun getExecutorData(env: String, clusterConf: ClusterConfig): ExecData {

        // loading the job configuration
        var envFile = File("repo/env/$env/job.yml")
        val envValue = if (envFile.exists()) {
            envFile.readText()
        } else {
            envFile = File("repo/env/$env/job.yaml")
            if (envFile.exists()) {
                envFile.readText()
            } else {
                ""
            }
        }

        val envData = ymlMapper.readValue<Environment>(envValue)
        // loading all additional configurations
        val files = File("repo/env/$env/").listFiles().filter { it.isFile }.filter { it.name != "job.yml" }
        val config = files.map { yamlToMap(it) }.toMap()

        // loading the job's dependencies
        var depsData: Dependencies? = null
        var pyDepsData: PythonDependencies? = null

        if (Files.exists(Paths.get("repo/deps/jars.yml"))) {
            val depsValue = File("repo/deps/jars.yml").readText()
            depsData = ymlMapper.readValue(depsValue)
        }
        if (Files.exists(Paths.get("repo/deps/python.yml"))) {
            val pyDepsValue = File("repo/deps/python.yml").readText()
            pyDepsData = ymlMapper.readValue(pyDepsValue)
        }

        return ExecData(envData, depsData, pyDepsData, config)
    }

    fun yamlToMap(file: File): Pair<String, Map<String, Any>> {

        val yaml = Yaml()
        val conf = yaml.load<Map<String, Any>>(FileInputStream(file))

        return file.name.replace(".yml", "") to conf
    }

    @JvmStatic
    fun getExecutorDataString(env: String, clusterConf: ClusterConfig): String {
        return mapper.writeValueAsString(getExecutorData(env, clusterConf))
    }

}