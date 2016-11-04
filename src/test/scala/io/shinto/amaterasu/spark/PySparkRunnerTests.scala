package io.shinto.amaterasu.spark

import io.shinto.amaterasu.runtime.Environment
import io.shinto.amaterasu.utils.TestNotifier
import org.scalatest.{FlatSpec, Matchers}
import io.shinto.amaterasu.execution.actions.runners.spark.PySpark.PySparkRunner

/**
  * Created by roadan on 9/2/16.
  */
class PySparkRunnerTests extends FlatSpec with Matchers {

  val env = new Environment()
  val notifier = new TestNotifier()


  val runner = PySparkRunner(env, notifier)

  "PySparkRunner.executeSource" should "execute simple python code" in {

    runner.executeSource(getClass.getResource("/simple-spark.py").getPath, "test_action")

  }

  it should "print and trows an errors" in {

    a [java.lang.Exception] should be thrownBy {
      runner.executeSource(getClass.getResource("/simple-spark-err.py").getPath, "test_action")
    }
  }

}
