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

import com.fasterxml.jackson.annotation.*
import org.apache.amaterasu.leader.common.configuration.Key

//import org.apache.amaterasu.common

@JsonIgnoreProperties(ignoreUnknown = true)
data class DataSetConfig(
        val file: Array<FileConfig> = emptyArray(),
        val hive: Array<HiveConfig> = emptyArray(),
        val generic: Array<GenericConfig> = emptyArray()
)

abstract class Config(val key: Key = Key()){

    abstract val name: String
    abstract val params: MutableMap<String, String>
}

data class GenericConfig(override val name: String,
                         @JsonAnySetter override val params: MutableMap<String, String> = mutableMapOf() ) : Config()

data class FileConfig(
        override val name: String,
        @JsonAnySetter override val params: MutableMap<String, String> = mutableMapOf(),
        val uri: String,
        val format: String,
        val mode: String
) : Config()

data class HiveConfig(
        override val name: String,
        @JsonAnySetter override val params: MutableMap<String, String> = mutableMapOf(),
        val uri: String,
        val format: String,
        val database: String,
        val table: String
) : Config()
