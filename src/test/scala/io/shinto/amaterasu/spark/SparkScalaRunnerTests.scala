package io.shinto.amaterasu.spark

import java.io.File

import io.shinto.amaterasu.configuration.environments.Environment
import io.shinto.amaterasu.configuration.ClusterConfig
import io.shinto.amaterasu.execution.actions.runners.spark.SparkScalaRunner

import org.apache.commons.io.FileUtils
import org.apache.hadoop.mapred.JobContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.{ SparkContext, SparkConf }

import org.scalatest.{ Matchers, FlatSpec }

import java.nio.file.{ Paths, Files }

class SparkScalaRunnerTests extends FlatSpec with Matchers {

  val script = getClass.getResource("/simple-spark.scala").getPath

  FileUtils.deleteQuietly(new File("/tmp/job_5/"))

  "SparkScalaRunner" should "execute the simple-spark.scala" in {
    val config = new ClusterConfig()
    config.load()

    val jar = config.Jar.replace("classes/", "") + "amaterasu-assembly-0.1.0.jar"
    println("checking for jar:")
    println(jar)
    println(Files.exists(Paths.get(jar)))
    val runner = SparkScalaRunner(config, "start", "job_5")
    val env = new Environment()
    env.workingDir = "file:///tmp"
    runner.execute(script, "test", env)
    //    val conf = new SparkConf(true)
    //      .setMaster(s"local[*]")
    //    val sc1 = new SparkContext(conf)
    //    import io.shinto.amaterasu.execution.AmaContext
    //    import org.apache.spark.sql.DataFrame
    //    AmaContext.init(sc1, new SQLContext(sc1), "job1", env)
    //    val data = Array(1, 2, 3, 4, 5)
    //
    //    val sc = AmaContext.sc
    //    val rdd = sc.parallelize(data)
    //    val sqlContext = AmaContext.sqlContext
    //
    //    import sqlContext.implicits._
    //    val x: DataFrame = rdd.toDF()
    //    AmaContext.saveDataFrame(x, "test", "x")
  }

}