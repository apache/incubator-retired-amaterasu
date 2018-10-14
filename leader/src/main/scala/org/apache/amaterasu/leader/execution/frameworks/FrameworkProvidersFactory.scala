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
package org.apache.amaterasu.leader.execution.frameworks

import org.apache.amaterasu.common.configuration.ClusterConfig
import org.apache.amaterasu.common.logging.Logging
import org.apache.amaterasu.sdk.frameworks.FrameworkSetupProvider
import org.reflections.Reflections

import scala.collection.JavaConversions._

class FrameworkProvidersFactory {
  var providers: Map[String, FrameworkSetupProvider] = _

  def groups: Array[String] = {
    providers.keys.toArray
  }

  def getFramework(groupId: String): FrameworkSetupProvider = {
    providers(groupId)
  }

}

object FrameworkProvidersFactory extends Logging {

  def apply(env: String, config: ClusterConfig): FrameworkProvidersFactory = {

    val result = new FrameworkProvidersFactory()

    val reflections = new Reflections(getClass.getClassLoader)
    val runnerTypes = reflections.getSubTypesOf(classOf[FrameworkSetupProvider]).toSet

    result.providers = runnerTypes.map(r => {

      val provider = Manifest.classType(r).runtimeClass.newInstance.asInstanceOf[FrameworkSetupProvider]

      provider.init(env, config)
      log.info(s"a provider for group ${provider.getGroupIdentifier} was created")
      (provider.getGroupIdentifier, provider)

    }).toMap

    result
  }
}