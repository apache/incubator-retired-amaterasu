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
package org.apache.amaterasu.leader.common.execution.frameworks

import org.apache.amaterasu.leader.common.configuration.ClusterConfig
import org.apache.amaterasu.common.logging.KLogging
import org.apache.amaterasu.sdk.frameworks.FrameworkSetupProvider
import org.reflections.Reflections

class FrameworkProvidersFactory(val env: String, val config: ClusterConfig) : KLogging() {

    val providers: MutableMap<String, FrameworkSetupProvider> = mutableMapOf()

    val groups: List<String> by lazy { providers.keys.toList() }

    fun getFramework(groupId: String): FrameworkSetupProvider = providers.getValue(groupId)

    init {
        val reflections =  Reflections(this::class.java.classLoader)
        val runnerTypes = reflections.getSubTypesOf(FrameworkSetupProvider::class.java)

        val loadedProviders = runnerTypes.map {

            val provider = it.newInstance()

            provider.init(env, config)
            log.info("a provider for group ${provider.groupIdentifier} was created")

            provider.groupIdentifier to provider

        }.toMap()

        providers.putAll(loadedProviders)
    }
}