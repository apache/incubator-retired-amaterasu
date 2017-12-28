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
package org.apache.amaterasu.executor.common.executors

import java.io.ByteArrayOutputStream

import org.apache.amaterasu.common.dataobjects.ExecData
import org.apache.amaterasu.common.execution.actions.Notifier
import org.apache.amaterasu.sdk.{AmaterasuRunner, RunnersProvider}
import org.reflections.Reflections

import scala.collection.JavaConversions._

//TODO: Check if we can use this in the YARN impl
class ProvidersFactory {

  var providers: Map[String, RunnersProvider] = _

  def getRunner(groupId: String, id: String): Option[AmaterasuRunner] = {
    val provider = providers.get(groupId)
    provider match {
      case Some(provider) => Some(provider.getRunner(id))
      case None => None
    }
  }
}

object ProvidersFactory {

  def apply(data: ExecData,
            jobId: String,
            outStream: ByteArrayOutputStream,
            notifier: Notifier,
            executorId: String,
            propFile:String = null): ProvidersFactory = {

    val result = new ProvidersFactory()
    val reflections = new Reflections(getClass.getClassLoader)
    val runnerTypes = reflections.getSubTypesOf(classOf[RunnersProvider]).toSet

    result.providers = runnerTypes.map(r => {

      val provider = Manifest.classType(r).runtimeClass.newInstance.asInstanceOf[RunnersProvider]

      provider.init(data, jobId, outStream, notifier, executorId, propFile)
      notifier.info(s"a provider for group ${provider.getGroupIdentifier} was created")
      (provider.getGroupIdentifier, provider)
    }).toMap

    result
  }

}