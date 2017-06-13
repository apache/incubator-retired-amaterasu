package io.shinto.amaterasu.RunnersTests

import java.io.ByteArrayOutputStream

import io.shinto.amaterasu.common.dataobjects.ExecData
import io.shinto.amaterasu.common.execution.dependencies.{Artifact, Dependencies, Repo}
import io.shinto.amaterasu.common.runtime.Environment
import io.shinto.amaterasu.executor.mesos.executors.ProvidersFactory
import io.shinto.amaterasu.utilities.TestNotifier
import org.apache.spark.repl.amaterasu.runners.spark.SparkScalaRunner
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.collection.mutable.ListBuffer

class RunnersLoadingTests extends FlatSpec with Matchers with BeforeAndAfterAll {

  var env: Environment = _
  var factory: ProvidersFactory = _

  override protected def beforeAll(): Unit = {
    env = Environment()
    env.workingDir = "file:///tmp/"
    env.master = "local[*]"
    factory = ProvidersFactory(ExecData(env, Dependencies(ListBuffer.empty[Repo], List.empty[Artifact]), null), "test", new ByteArrayOutputStream(), new TestNotifier(), "test")
    super.beforeAll()
  }

  override protected def afterAll(): Unit = {

    factory.getRunner("spark", "scala").get.asInstanceOf[SparkScalaRunner].spark.stop()

    super.afterAll()
  }


  "RunnersFactory" should "be loaded with all the implementations of AmaterasuRunner in its classpath" in {
    val r = factory.getRunner("spark", "scala")
    r should not be null
  }
}
