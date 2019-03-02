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
package org.apache.amaterasu.frameworks.jvm.common.configuration.dataset

import java.io.File
import org.yaml.snakeyaml.Yaml
import java.io.FileInputStream
import kotlin.reflect.KClass
import kotlin.reflect.KParameter
import kotlin.reflect.full.primaryConstructor

class DataSetConfigManager(env: String, repoPath: String) {

    private val envFolder = "$repoPath/env/$env"

    lateinit var config: Map<String, Config>

    init {
        config = File(envFolder)
                .listFiles(DATASET_YAML_FILE_FILTER)
                .fold(mapOf<String, Config>()) { acc, file -> acc + parseConfigs(file!!) }
    }

    private inline fun parseConfigs(file: File): Map<String, Config> {
        FileInputStream(file).use { iStream ->
            val rawConfigs: Map<String, List<Map<String, String>>> = Yaml().load(iStream)
            val parsedConfigs = rawConfigs.map { entry ->
                when (entry.key) {
                    "file" -> convertMapToConfigType(entry.value, FileConfig::class)
                    "hive" -> convertMapToConfigType(entry.value, HiveConfig::class)
                    "generic" -> convertMapToConfigType(entry.value, GenericConfig::class)
                    else -> convertMapToConfigType(entry.value, GenericConfig::class)
                }
            }
            return parsedConfigs.fold(mutableMapOf<String, Config>()) { acc, curr -> acc.putAll(curr); acc }.toMap()
        }
    }

    private inline fun <reified T : Config> convertMapToConfigType(
            values: List<Map<String, String>>,
            kClass: KClass<T>
    ): Map<String, T> {
        val constructor = kClass.primaryConstructor
                ?: throw RuntimeException("A primary constructor must be defined for the config type class")

        return values.map { eachMap ->
            lateinit var paramKParam: KParameter
            val paramsValue = mutableMapOf<String, String>()
            val arguments = constructor.parameters.map { param ->
                if (param.name == "params") paramKParam = param
                val key = param.name!!
                val value = eachMap.getOrElse(key) { "" }
                paramsValue.put(key, value)
                param to value
            }.toMap()

            val constructorParam = mapOf(paramKParam to paramsValue)
            if (!eachMap.containsKey("name")) throw RuntimeException("Each configuration must have a 'name' attribute")
            eachMap.get("name")!! to constructor.callBy(arguments + constructorParam)
        }.toMap()

    }

    fun getAllConfigs(): Collection<Config> {
        return config.values
    }

    inline fun <reified T : Config> getConfigs(): List<T>? {
        return config.values.filterIsInstance(T::class.java).toList()
    }

    inline fun <reified T : Config> getConfigByName(name: String): T {
        return config.get(name)?.takeIf { config -> config.javaClass == T::class.java } as T
    }

    companion object {
        private val ALLOWED_DATA_FORMATS = listOf("file", "hive") //TODO - Identify which config to move this to
        private val DATASETS_PREFIX = "datasets"
        private val DATASET_FILE_PATTERN = """(datasets)(.*)+(.yml|.yaml)$""".toRegex(RegexOption.IGNORE_CASE)
        val DATASET_YAML_FILE_FILTER = { file: File ->
            (file.isFile && DATASET_FILE_PATTERN.find(file.name)?.value?.isNotBlank() ?: false)
        }
    }
}
