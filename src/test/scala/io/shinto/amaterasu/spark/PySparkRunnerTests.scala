package io.shinto.amaterasu.spark

import io.shinto.amaterasu.runtime.Environment
import io.shinto.amaterasu.utils.TestNotifier
import io.shinto.amaterasu.execution.actions.runners.spark.PySpark.PySparkRunner
import org.apache.spark.{SparkConf, SparkContext, SparkEnv}
import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.scalatest.{FlatSpec, Matchers}

/**
  * Created by roadan on 9/2/16.
  */
class PySparkRunnerTests extends FlatSpec with Matchers {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  Logger.getLogger("spark").setLevel(Level.OFF)
  Logger.getLogger("jetty").setLevel(Level.OFF)


  Logger.getRootLogger.setLevel(Level.OFF)
  val env = new Environment()
  val notifier = new TestNotifier()

  val conf = new SparkConf(true)
    .setMaster("local[*]")
    .setAppName("job_5")

  val sc = new SparkContext(conf)

  sc.setLogLevel("ERROR")

  val runner = PySparkRunner(env, "job_5", notifier, sc)

  "PySparkRunner.executeSource" should "execute simple python code" in {
    runner.executeSource(getClass.getResource("/simple-python.py").getPath, "test_action1")
  }

//  it should "print and trows an errors" in {
//    a[java.lang.Exception] should be thrownBy {
//      runner.executeSource(getClass.getResource("/simple-python-err.py").getPath, "test_action2")
//    }
//  }
//
//  it should "also execute spark code written in python" in {
//    runner.executeSource(getClass.getResource("/simple-pyspark.py").getPath, "test_action3")
//  }

//  it should "also execute spark code written in python with AmaContext being used" in {
//    runner.executeSource(getClass.getResource("/pyspark-with-amacontext.py").getPath, "test_action4")
//  }

  sc.stop()
}
