package io.shinto.amaterasu.spark

import java.io.File

import io.shinto.amaterasu.executor.execution.actions.runners.spark.PySpark.PySparkRunner
import io.shinto.amaterasu.common.runtime.Environment
import io.shinto.amaterasu.utilities.TestNotifier

import org.apache.spark.sql.SparkSession

import org.scalatest._

import scala.collection.JavaConverters._

@DoNotDiscover
class PySparkRunnerTests extends FlatSpec with Matchers with BeforeAndAfterAll {


  var runner: PySparkRunner = _
  var spark: SparkSession = _
  var env: Environment = _

  override protected def beforeAll(): Unit = {


    val notifier = new TestNotifier()

    // this is an ugly hack, getClass.getResource("/").getPath should have worked but
    // stopped working when we moved to gradle :(
    val resources = new File(getClass.getResource("/spark_intp.py").getPath).getParent

    runner = PySparkRunner(env, "job_5", notifier, spark, resources)

    super.beforeAll()
  }


  "PySparkRunner.executeSource" should "execute simple python code" in {
    runner.executeSource(getClass.getResource("/simple-python.py").getPath, "test_action1", Map.empty[String, String].asJava)
  }

  it should "print and trows an errors" in {
    a[java.lang.Exception] should be thrownBy {
      runner.executeSource(getClass.getResource("/simple-python-err.py").getPath, "test_action2", Map.empty[String, String].asJava)
    }
  }

  it should "also execute spark code written in python" in {
    runner.executeSource(getClass.getResource("/simple-pyspark.py").getPath, "test_action3", Map.empty[String, String].asJava)
  }

  it should "also execute spark code written in python with AmaContext being used" in {
    runner.executeSource(getClass.getResource("/pyspark-with-amacontext.py").getPath, "test_action4", Map.empty[String, String].asJava)
  }

}