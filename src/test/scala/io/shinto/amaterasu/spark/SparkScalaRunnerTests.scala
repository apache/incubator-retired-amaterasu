package io.shinto.amaterasu.spark

import io.shinto.amaterasu.configuration.SparkConfig
import io.shinto.amaterasu.execution.actions.runners.spark.SparkScalaRunner
import org.scalatest.{ Matchers, FlatSpec }

class SparkScalaRunnerTests extends FlatSpec with Matchers {

  val script = getClass.getResource("/simple-spark.scala").getPath

  "SparkScalaRunner" should "execute the simple-spark.scala" in {

    val runner = SparkScalaRunner(new SparkConfig(), "start", "job_1")
    runner.execute(script, null, "test")

  }

}