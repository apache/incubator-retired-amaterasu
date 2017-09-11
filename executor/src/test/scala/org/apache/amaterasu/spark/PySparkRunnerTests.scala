package io.shinto.amaterasu.spark

import java.io.File

import org.apache.amaterasu.common.runtime.Environment
import org.apache.amaterasu.executor.execution.actions.runners.spark.PySpark.PySparkRunner
import org.apache.amaterasu.executor.mesos.executors.ProvidersFactory
import org.apache.amaterasu.utilities.TestNotifier
import org.apache.log4j.{Level, Logger}
import org.apache.spark.repl.amaterasu.runners.spark.SparkRunnerHelper
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest.{BeforeAndAfterAll, DoNotDiscover, FlatSpec, Matchers}

import scala.collection.JavaConverters._
import scala.io.Source

@DoNotDiscover
class PySparkRunnerTests extends FlatSpec with Matchers with BeforeAndAfterAll {

  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  Logger.getLogger("spark").setLevel(Level.OFF)
  Logger.getLogger("jetty").setLevel(Level.OFF)
  Logger.getRootLogger.setLevel(Level.OFF)

  var factory: ProvidersFactory = _

  def delete(file: File) {
    if (file.isDirectory)
      Option(file.listFiles).map(_.toList).getOrElse(Nil).foreach(delete(_))
    file.delete
  }

  override protected def afterAll(): Unit = {
    val pysparkDir = new File(getClass.getResource("/pyspark").getPath)
    val py4jDir = new File(getClass.getResource("/py4j").getPath)
    delete(pysparkDir)
    delete(py4jDir)
    super.afterAll()
  }


  "PySparkRunner.executeSource" should "execute simple python code" in {
    val src = Source.fromFile(getClass.getResource("/simple-python.py").getPath).mkString
    var runner = factory.getRunner("spark", "pyspark").get.asInstanceOf[PySparkRunner]
    println("3333333333333333333333")
    runner.executeSource(src, "test_action1", Map.empty[String, String].asJava)
  }

  it should "print and trows an errors" in {
    a[java.lang.Exception] should be thrownBy {
      val src = Source.fromFile(getClass.getResource("/simple-python-err.py").getPath).mkString
      var runner = factory.getRunner("spark", "pyspark").get.asInstanceOf[PySparkRunner]
      runner.executeSource(src, "test_action2", Map.empty[String, String].asJava)
    }
  }

  it should "also execute spark code written in python" in {
    val src = Source.fromFile(getClass.getResource("/simple-pyspark.py").getPath).mkString
    var runner = factory.getRunner("spark", "pyspark").get.asInstanceOf[PySparkRunner]
    runner.executeSource(src, "test_action3", Map("numDS" -> "parquet").asJava)
  }

  it should "also execute spark code written in python with AmaContext being used" in {
    val src = Source.fromFile(getClass.getResource("/pyspark-with-amacontext.py").getPath).mkString
    var runner = factory.getRunner("spark", "pyspark").get.asInstanceOf[PySparkRunner]
    runner.executeSource(src, "test_action4", Map.empty[String, String].asJava)
  }

}