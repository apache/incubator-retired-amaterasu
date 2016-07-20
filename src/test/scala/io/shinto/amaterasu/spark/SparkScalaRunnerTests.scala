//package io.shinto.amaterasu.spark
//
//import java.io.File
//
//import io.shinto.amaterasu.configuration.environments.Environment
//import io.shinto.amaterasu.configuration.ClusterConfig
//import io.shinto.amaterasu.execution.actions.runners.spark.SparkScalaRunner
//
//import org.apache.commons.io.FileUtils
//import org.apache.spark.{ SparkContext, SparkConf }
//import org.apache.spark.repl.Main
//
//import org.scalatest.{ Matchers, FlatSpec }
//
//class SparkScalaRunnerTests extends FlatSpec with Matchers {
//
//  FileUtils.deleteQuietly(new File("/tmp/job_5/"))
//
//  val config = new ClusterConfig()
//  config.load()
//
//  val env = new Environment()
//  env.workingDir = "file:///tmp"
//  env.master = "local[*]"
//
//  val conf = new SparkConf(true)
//    .setMaster(env.master)
//    .setAppName("job_5")
//    .set("spark.executor.uri", "http://mirror.ventraip.net.au/apache/spark/spark-1.6.1/spark-1.6.1-bin-hadoop2.4.tgz")
//    .set("spark.repl.class.uri", Main.getClass().getName) //TODO: :\ check this
//  val sc = new SparkContext(conf)
//
//  val runner = SparkScalaRunner(env, "job_5", sc)
//
//  "SparkScalaRunner" should "execute the simple-spark.scala" in {
//
//    val script = getClass.getResource("/simple-spark.scala").getPath
//
//    runner.execute(script, "start")
//
//  }
//
//  "SparkScalaRunner" should "execute step-2.scala and access data from simple-spark.scala" in {
//
//    val script = getClass.getResource("/step-2.scala").getPath
//
//    runner.execute(script, "cont")
//
//  }
//}