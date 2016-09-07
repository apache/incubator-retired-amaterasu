package io.shinto.amaterasu.execution.actions.runners.spark

import io.shinto.amaterasu.runtime.Environment
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.repl.amaterasu.runners.spark.SparkScalaRunner._

/**
  * Created by eyalbenivri on 02/09/2016.
  */
object SparkRunnerHelper {
  def createSparkContext(env: Environment, sparkAppName: String, classServerUri: String, jars: Seq[String]): SparkContext = {

    val conf = new SparkConf(true)
      .setMaster(env.master)
      .setAppName(sparkAppName)
      .set("spark.executor.uri", s"http://${sys.env("AMA_NODE")}:8000/spark-1.6.1-2.tgz")
      .set("spark.driver.memory", "512m")
      .set("spark.repl.class.uri", classServerUri)
      .set("spark.mesos.coarse", "true")
      .set("spark.executor.instances", "2")
      .set("spark.cores.max", "5")
      .set("spark.hadoop.validateOutputSpecs", "false")
    val sc = new SparkContext(conf)
    for (jar <- jars) {
      sc.addJar(jar) // and this is how my childhood was ruined :(
    }
    val hc = sc.hadoopConfiguration

    if (!sys.env("AWS_ACCESS_KEY_ID").isEmpty &&
      !sys.env("AWS_SECRET_ACCESS_KEY").isEmpty) {

      hc.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
      hc.set("fs.s3n.awsAccessKeyId", sys.env("AWS_ACCESS_KEY_ID"))
      hc.set("fs.s3n.awsSecretAccessKey", sys.env("AWS_SECRET_ACCESS_KEY"))
    }
    sc

  }
}
