package io.shinto.amaterasu.spark

import java.io.File

import io.shinto.amaterasu.configuration.environments.Environment
import io.shinto.amaterasu.configuration.ClusterConfig
import io.shinto.amaterasu.execution.actions.runners.spark.SparkScalaRunner

import org.apache.commons.io.FileUtils

import org.scalatest.{ Matchers, FlatSpec }

class SparkScalaRunnerTests extends FlatSpec with Matchers {

  FileUtils.deleteQuietly(new File("/tmp/job_5/"))

  val config = new ClusterConfig()
  config.load()

  val env = new Environment()
  env.workingDir = "file:///tmp"
  env.master = "local[*]"

  val runner = SparkScalaRunner(env, "job_5")

  "SparkScalaRunner" should "execute the simple-spark.scala" in {

    val script = getClass.getResource("/simple-spark.scala").getPath

    runner.execute(script, "start")

  }

  "SparkScalaRunner" should "execute step-2.scala and access data from simple-spark.scala" in {

    val script = getClass.getResource("/step-2.scala").getPath

    runner.execute(script, "cont")

  }
}