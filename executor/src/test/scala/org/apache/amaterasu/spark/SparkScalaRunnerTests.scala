//package org.apache.amaterasu.spark
//
//import java.io.File
//
//import org.apache.amaterasu.common.runtime._
//import org.apache.amaterasu.common.configuration.ClusterConfig
//import org.apache.amaterasu.utilities.TestNotifier
//
//import scala.collection.JavaConverters._
//import org.apache.commons.io.FileUtils
//import java.io.ByteArrayOutputStream
//
//import org.apache.spark.SparkConf
//import org.apache.spark.repl.Main
//import org.apache.spark.repl.amaterasu.runners.spark.{SparkRunnerHelper, SparkScalaRunner}
//import org.apache.spark.sql.SparkSession
//import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}
//
//class SparkScalaRunnerTests extends FlatSpec with Matchers with BeforeAndAfterAll {
//
//  var runner: SparkScalaRunner = _
//
//  override protected def beforeAll(): Unit = {
//
//    FileUtils.deleteQuietly(new File("/tmp/job_5/"))
//
//    val env = Environment()
//    env.workingDir = "file:///tmp"
//    env.master = "local[*]"
//
//
//    val spark = SparkRunnerHelper.createSpark(env, "job_5", Seq.empty[String], Map.empty)
//
//
//    val notifier = new TestNotifier()
//    val strm = new ByteArrayOutputStream()
//    runner = SparkScalaRunner(env, "job_5", spark, strm, notifier, Seq.empty[String])
//    super.beforeAll()
//  }
//
//  "SparkScalaRunner" should "execute the simple-spark.scala" in {
//
//    val script = getClass.getResource("/simple-spark.scala").getPath
//    runner.executeSource(script, "start", Map.empty[String, String].asJava)
//
//  }
//
//  "SparkScalaRunner" should "execute step-2.scala and access data from simple-spark.scala" in {
//
//    val script = getClass.getResource("/step-2.scala").getPath
//    runner.executeSource(script, "cont", Map.empty[String, String].asJava)
//
//  }
//}