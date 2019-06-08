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

import com.fasterxml.jackson.core.JsonParser
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.kotlin.KotlinModule
import java.io.File
import java.io.FileInputStream

class DataSetConfigManager(env: String, repoPath: String) {

    private val envFolder = "$repoPath/env/$env"
    private val mapper = let {
        val mapper = ObjectMapper(YAMLFactory())
        mapper.registerModule(KotlinModule())
        mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true)
        mapper.enableDefaultTyping(ObjectMapper.DefaultTyping.OBJECT_AND_NON_CONCRETE)
        mapper.enableDefaultTyping(ObjectMapper.DefaultTyping.NON_CONCRETE_AND_ARRAYS)
        mapper
    }

    var config: DataSetConfig

    init {
        config = File(envFolder)
                .listFiles(DATASET_YAML_FILE_FILTER)
                .fold(DataSetConfig()) { root, file -> mergeConfig(root, parseConfigs(file)) }
    }

    private fun mergeConfig(acc: DataSetConfig?, curr: DataSetConfig?): DataSetConfig {
        return if (acc != null && curr != null) {
            acc.copy(file = acc.file.plus(curr.file),
                    hive = acc.hive.plus(curr.hive),
                    generic = acc.generic.plus(curr.generic)
            )

        } else if (acc != null) {
            acc
        } else curr!!
    }

    private inline fun parseConfigs(file: File?): DataSetConfig? {
        FileInputStream(file).use { iStream ->
            return mapper.readValue(iStream, DataSetConfig::class.java)
        }
    }

    fun getFileConfigs(): List<FileConfig> {
        return config.file.asList()
    }

    fun getHiveConfigs(): List<HiveConfig> {
        return config.hive.asList()
    }

    fun getGenericConfigs(): List<GenericConfig> {
        return config.generic.asList()
    }

    fun getFileConfigByName(name: String): FileConfig? {
        return getFileConfigs().find { conf -> conf.name == name }
    }

    fun getHiveConfigByName(name: String): HiveConfig? {
        return getHiveConfigs().find { conf -> conf.name == name }
    }

    fun getGenericConfigByName(name: String): GenericConfig? {
        return getGenericConfigs().find { conf -> conf.name == name }
    }

    companion object {
        private val DATASET_FILE_PATTERN = """(datasets)(.*)+(.yml|.yaml)$""".toRegex(RegexOption.IGNORE_CASE)
        val DATASET_YAML_FILE_FILTER = { file: File ->
            (file.isFile && DATASET_FILE_PATTERN.find(file.name)?.value?.isNotBlank() ?: false)
        }
    }
}
