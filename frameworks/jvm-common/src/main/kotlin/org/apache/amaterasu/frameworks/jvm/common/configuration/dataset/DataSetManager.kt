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

import com.uchuhimo.konf.Config
import com.uchuhimo.konf.source.yaml.toYaml
import java.io.File

class DataSetManager(private val env: String, private val repoPath: String, private val frameworkItems: List<String> = emptyList()) {

    private val envFolder = "$repoPath/env/$env"

    private var config: Config = Config {
        addSpec(DataSetsSpec("datasets").spec)
    }

    init {
        for (file in File(envFolder).listFiles(DATASET_YAML_FILE_FILTER)) {
            config = config.from.yaml.file(file)
            println(config.toYaml.toText())
        }
    }

    fun getConfigsByType(typee: String): Map<String, Map<String, String>>? {
        return config.get<Map<String, Map<String,Map<String,String>>>>(DATASETS_PREFIX).get(typee)
    }

    fun getConfigsByName(typee: String, dataSetName: String): Map<String,String>? {
        return config.get<Map<String, Map<String,Map<String,String>>>>(DATASETS_PREFIX).get(typee)?.get(dataSetName)
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
