package io.shinto.amaterasu.RunnersTests

import io.shinto.amaterasu.common.runtime.Environment
import io.shinto.amaterasu.executor.mesos.executors.ProvidersFactory
import org.scalatest._

@DoNotDiscover
class RunnersLoadingTests extends FlatSpec with Matchers with BeforeAndAfterAll {

  var env: Environment = _
  var factory: ProvidersFactory = _

  "RunnersFactory" should "be loaded with all the implementations of AmaterasuRunner in its classpath" in {
    val r = factory.getRunner("spark", "scala")
    r should not be null
  }
}


