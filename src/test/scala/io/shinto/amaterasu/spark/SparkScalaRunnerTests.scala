package io.shinto.amaterasu.spark

import io.shinto.amaterasu.configuration.environments.Environment
import io.shinto.amaterasu.configuration.ClusterConfig
import io.shinto.amaterasu.execution.actions.runners.spark.SparkScalaRunner

import org.scalatest.{ Matchers, FlatSpec }
import scalax.file.Path

class SparkScalaRunnerTests extends FlatSpec with Matchers {

  val script = getClass.getResource("/simple-spark.scala").getPath

  val path = Path.fromString("/tmp/job_1/")

  path.deleteRecursively(continueOnFailure = true)

  "SparkScalaRunner" should "execute the simple-spark.scala" in {

    val runner = SparkScalaRunner(new ClusterConfig(), "start", "job_1")
    val env = new Environment()
    env.workingDir = "file:///tmp"
    runner.execute(script, null, "test", env)

  }

}