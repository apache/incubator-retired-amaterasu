package org.apache.spark.repl.amaterasu.runners.spark

import java.io.{ByteArrayOutputStream, File, PrintWriter}

import io.shinto.amaterasu.common.configuration.ClusterConfig
import io.shinto.amaterasu.common.execution.actions.Notifier
import io.shinto.amaterasu.common.runtime.Environment
import org.apache.spark.repl.amaterasu.AmaSparkILoop
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.Utils

import scala.tools.nsc.{GenericRunnerSettings, Settings}
import scala.tools.nsc.interpreter.IMain


/**
  * Created by eyalbenivri on 02/09/2016.
  */
object SparkRunnerHelper {

  private val conf = new SparkConf()
  private val rootDir = conf.getOption("spark.repl.classdir").getOrElse(Utils.getLocalDir(conf))
  private val outputDir = Utils.createTempDir(root = rootDir, namePrefix = "repl")

  private var sparkContext: SparkContext = _
  private var sparkSession: SparkSession = _

  var notifier: Notifier = _
  //private var interpreter: IMain = null

  //var classServerUri: String = null
  private var interpreter: IMain = _

  def getNode: String = sys.env.get("AMA_NODE") match {
    case None => "127.0.0.1"
    case _ => sys.env("AMA_NODE")
  }

  def getOrCreateScalaInterperter(outStream: ByteArrayOutputStream, jars: Seq[String], recreate: Boolean = false): IMain = {
    if (interpreter == null || recreate) {
      initInterpreter(outStream, jars)
    }
    interpreter
  }

  private def scalaOptionError(msg: String): Unit = {
    notifier.error("", msg)
  }

  private def initInterpreter(outStream: ByteArrayOutputStream, jars: Seq[String]) = {

    var result: IMain = null
    //var classServerUri: String = null
    val config = new ClusterConfig()
    try {
      //val command = new SparkCommandLine(List())

      val interpArguments = List(
        "-Yrepl-class-based",
        "-Yrepl-outdir", s"${outputDir.getAbsolutePath}",
        "-classpath", jars.mkString(File.separator)
      )

      val settings = new GenericRunnerSettings(scalaOptionError)
      settings.processArguments(interpArguments, true)

      settings.classpath.append(System.getProperty("java.class.path") + java.io.File.pathSeparator +
        "spark-" + config.Webserver.sparkVersion + "/jars/*" + java.io.File.pathSeparator +
        jars.mkString(java.io.File.pathSeparator))

      settings.usejavacp.value = true

      //val in: Option[BufferedReader] = null
      val out = new PrintWriter(outStream)
      val interpreter = new AmaSparkILoop(out)
      interpreter.setSttings(settings)

      interpreter.create

      val intp = interpreter.getIntp

      settings.embeddedDefaults(Thread.currentThread().getContextClassLoader)
      intp.setContextClassLoader
      intp.initializeSynchronous

      result = intp
    }
    catch {
      case e: Exception =>
        println("+++++++>" + new Predef.String(outStream.toByteArray))

    }

    interpreter = result
  }

  def createSpark(env: Environment, sparkAppName: String, jars: Seq[String], sparkConf: Map[String, Any], executorEnv: Map[String, Any]): SparkSession = {

    val config = new ClusterConfig()

    Thread.currentThread().setContextClassLoader(getClass.getClassLoader)

    conf.setAppName(sparkAppName)
      .set("spark.master", env.master)
      .set("spark.executor.uri", s"http://$getNode:${config.Webserver.Port}/spark-2.1.1-bin-hadoop2.7.tgz")
      .set("spark.driver.host", getNode)
      .set("spark.submit.deployMode", "client")
      .set("spark.home", s"${scala.reflect.io.File(".").toCanonical.toString}/spark-2.1.1-bin-hadoop2.7")
      .set("spark.hadoop.validateOutputSpecs", "false")

      //.setExecutorEnv("PYTHONPATH", pysparkPath)
      .setJars(jars)

    // adding the the configurations from spark.yml
    for (c <- sparkConf) {
      if (c._2.isInstanceOf[String])
        conf.set(c._1, c._2.toString)
    }

    // setting the executor env from spark_exec.yml
    for (c <- executorEnv) {
      if (c._2.isInstanceOf[String])
        conf.setExecutorEnv(c._1, c._2.toString)
    }

    conf.set("spark.repl.class.outputDir", outputDir.getAbsolutePath)

    sparkSession = SparkSession.builder
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
