package org.apache.amaterasu.leader.execution.frameworks

import java.net.{URL, URLClassLoader}

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

  def apply(config: ClusterConfig): FrameworkProvidersFactory = {

    val result = new FrameworkProvidersFactory()

    val reflections = new Reflections(getClass.getClassLoader)
    val runnerTypes = reflections.getSubTypesOf(classOf[FrameworkSetupProvider]).toSet

    result.providers = runnerTypes.map(r => {

      val provider = Manifest.classType(r).runtimeClass.newInstance.asInstanceOf[FrameworkSetupProvider]

      provider.init(config)
      log.info(s"a provider for group ${provider.getGroupIdentifier} was created")
      (provider.getGroupIdentifier, provider)

    }).toMap

    result
  }
}