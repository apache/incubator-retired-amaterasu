package io.shinto.amaterasu.spark

import io.shinto.amaterasu.runtime.{AmaContext, Environment}
import io.shinto.amaterasu.utils.TestNotifier
import io.shinto.amaterasu.execution.actions.runners.spark.PySpark.PySparkRunner
import org.apache.spark.sql.SQLContext

import org.apache.spark.{SparkContext, SparkConf}

import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by roadan on 9/2/16.
  */
class PySparkRunnerTests extends FlatSpec with Matchers {

  val env = new Environment()
  val notifier = new TestNotifier()

  val conf = new SparkConf(true)
      .setMaster("local[*]")
      .setAppName("job_5")

  val sc = new SparkContext(conf)

  val runner = PySparkRunner(env, notifier, sc)

  "PySparkRunner.executeSource" should "execute simple python code" in {
    runner.executeSource(getClass.getResource("/simple-python.py").getPath, "test_action")
  }

  it should "print and trows an errors" in {
    a [java.lang.Exception] should be thrownBy {
      runner.executeSource(getClass.getResource("/simple-python-err.py").getPath, "test_action")
    }
  }

  it should "also execute spark code written in python" in {
    runner.executeSource(getClass.getResource("/simple-pyspark.py").getPath, "test_action")
  }
}
