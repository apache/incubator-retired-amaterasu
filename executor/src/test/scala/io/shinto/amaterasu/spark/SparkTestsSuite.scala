package io.shinto.amaterasu.spark

import java.io.{ByteArrayOutputStream, File}

import io.shinto.amaterasu.RunnersTests.RunnersLoadingTests
import io.shinto.amaterasu.common.dataobjects.ExecData
import io.shinto.amaterasu.common.execution.dependencies.{Artifact, Dependencies, Repo}
import io.shinto.amaterasu.common.runtime.Environment
import io.shinto.amaterasu.executor.mesos.executors.ProvidersFactory
import io.shinto.amaterasu.utilities.TestNotifier
import org.apache.spark.repl.amaterasu.runners.spark.SparkScalaRunner
import org.apache.spark.sql.SparkSession
import org.scalatest._

import scala.collection.mutable.ListBuffer

/**
  * Created by roadan on 22/6/17.
  */
class SparkTestsSuite extends Suites(
  new PySparkRunnerTests(),
  new RunnersLoadingTests()) with BeforeAndAfterAll {

  var env: Environment = _
  var factory: ProvidersFactory = _
  var spark: SparkSession = _

  override def beforeAll(): Unit = {

    env = Environment()
    env.workingDir = "file:///tmp/"
    env.master = "local[*]"

    // I can't apologise enough for this
    val resources = new File(getClass.getResource("/spark_intp.py").getPath).getParent
    factory = ProvidersFactory(ExecData(env, Dependencies(ListBuffer.empty[Repo], List.empty[Artifact]), Map("spark" -> Map.empty[String, Any],"spark_exec"->Map("PYTHONPATH"->resources))), "test", new ByteArrayOutputStream(), new TestNotifier(), "test")
    spark = factory.getRunner("spark", "scala").get.asInstanceOf[SparkScalaRunner].spark

    this.nestedSuites.filter(s => s.isInstanceOf[RunnersLoadingTests]).foreach(s => s.asInstanceOf[RunnersLoadingTests].factory = factory)
    this.nestedSuites.filter(s => s.isInstanceOf[PySparkRunnerTests]).foreach(s => {
      s.asInstanceOf[PySparkRunnerTests].spark = spark
      s.asInstanceOf[PySparkRunnerTests].env = env
    })

    super.beforeAll()
  }

  override def afterAll(): Unit = {
    spark.stop()

    super.afterAll()
  }

}
