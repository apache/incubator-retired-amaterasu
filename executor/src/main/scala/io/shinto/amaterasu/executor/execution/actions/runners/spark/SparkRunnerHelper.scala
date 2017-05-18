package io.shinto.amaterasu.executor.execution.actions.runners.spark

import java.util.Properties

import io.shinto.amaterasu.common.configuration.ClusterConfig
import io.shinto.amaterasu.common.runtime.Environment
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.JavaConversions._

/**
  * Created by eyalbenivri on 02/09/2016.
  */
object SparkRunnerHelper {

  def getNode: String = sys.env.get("AMA_NODE") match {
    case None => "127.0.0.1"
    case _ => sys.env("AMA_NODE")
  }

  def createSpark(env: Environment, sparkAppName: String, jars: Seq[String]): SparkSession = {

    val config = new ClusterConfig()

    Thread.currentThread().setContextClassLoader(getClass.getClassLoader())

    val conf = new SparkConf(true)
      .set("spark.executor.uri", s"http://$getNode:${config.Webserver.Port}/spark-${config.Webserver.sparkVersion}.tgz")
      //      .set("spark.driver.memory", "512m")
      //.set("spark.repl.class.uri", classServerUri)
      //      .set("spark.mesos.coarse", "true")
      //      .set("spark.executor.instances", "2")
      //      .set("spark.cores.max", "5")
      .set("spark.hadoop.validateOutputSpecs", "false")
      .setJars(jars)

    val sparkSession = SparkSession.builder
      .appName(sparkAppName)
      .master(env.master)

      //.enableHiveSupport()
      .config(conf).getOrCreate()

    val hc = sparkSession.sparkContext.hadoopConfiguration

    sys.env.get("AWS_ACCESS_KEY_ID") match {
      case None =>
      case _ =>
        hc.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
        hc.set("fs.s3n.awsAccessKeyId", sys.env("AWS_ACCESS_KEY_ID"))
        hc.set("fs.s3n.awsSecretAccessKey", sys.env("AWS_SECRET_ACCESS_KEY"))
    }
    sparkSession
  }
}
