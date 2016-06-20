package io.shinto.amaterasu.spark

import java.io.File

import io.shinto.amaterasu.configuration.environments.Environment
import io.shinto.amaterasu.configuration.ClusterConfig
import io.shinto.amaterasu.execution.actions.runners.spark.SparkScalaRunner
import org.apache.commons.io.FileUtils

import org.scalatest.{ Matchers, FlatSpec }

class SparkScalaRunnerTests extends FlatSpec with Matchers {

  val script = getClass.getResource("/simple-spark.scala").getPath

  FileUtils.deleteQuietly(new File("/tmp/job_5/"))

  "SparkScalaRunner" should "execute the simple-spark.scala" in {

    val runner = SparkScalaRunner(new ClusterConfig(), "start", "job_5")
    val env = new Environment()
    env.workingDir = "file:///tmp"
    runner.execute(script, null, "test", env)

  }

}