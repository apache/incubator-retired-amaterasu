package io.shinto.amaterasu.spark

import io.shinto.amaterasu.runtime.Environment
import io.shinto.amaterasu.utils.TestNotifier
import io.shinto.amaterasu.execution.actions.runners.spark.PySpark.PySparkRunner
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FlatSpec, Matchers}

/**
  * Created by roadan on 9/2/16.
  */
class PySparkRunnerTests extends FlatSpec with Matchers with BeforeAndAfterAll {

  var sc: SparkContext = _
  var runner: PySparkRunner = _

  override protected def beforeAll(): Unit = {
    val env = new Environment()
    val notifier = new TestNotifier()

    val conf = new SparkConf(true)
      .setMaster("local[*]")
      .setAppName("job_5")
      .setExecutorEnv("PYTHONPATH", getClass.getResource("/").getPath)

    sc = new SparkContext(conf)

    sc.setLogLevel("ERROR")

    runner = PySparkRunner(env, "job_5", notifier, sc, getClass.getResource("/").getPath)

    super.beforeAll()
  }

  override protected def afterAll(): Unit = {
    sc.stop()

    super.afterAll()
  }


  "PySparkRunner.executeSource" should "execute simple python code" in {
    runner.executeSource(getClass.getResource("/simple-python.py").getPath, "test_action1")
  }

  it should "print and trows an errors" in {
    a[java.lang.Exception] should be thrownBy {
      runner.executeSource(getClass.getResource("/simple-python-err.py").getPath, "test_action2")
    }
  }


  it should "also execute spark code written in python" in {
    runner.executeSource(getClass.getResource("/simple-pyspark.py").getPath, "test_action3")
  }

  it should "also execute spark code written in python with AmaContext being used" in {
    runner.executeSource(getClass.getResource("/pyspark-with-amacontext.py").getPath, "test_action4")
  }

}
