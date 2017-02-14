package io.shinto.amaterasu.executor.mesos.executors

import java.io.ByteArrayOutputStream

import io.shinto.amaterasu.common.dataobjects.ExecData
import io.shinto.amaterasu.common.execution.actions.Notifier
import io.shinto.amaterasu.sdk.{AmaterasuRunner, RunnersProvider}
import org.reflections.Reflections

import scala.collection.JavaConversions._

/**
  * Created by roadan on 2/6/17.
  */
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
            executorId: String): ProvidersFactory = {

    val result = new ProvidersFactory()
    val reflections = new Reflections(getClass.getClassLoader)
    val runnerTypes = reflections.getSubTypesOf(classOf[RunnersProvider]).toSet

    result.providers = runnerTypes.map(r => {

      val provider = Manifest.classType(r).runtimeClass.newInstance.asInstanceOf[RunnersProvider]

      notifier.info(s"a provider for group ${provider.getGroupIdentifier} was created")
      provider.init(data, jobId, outStream, notifier, executorId)
      (provider.getGroupIdentifier, provider)
    }).toMap

    result
  }

}