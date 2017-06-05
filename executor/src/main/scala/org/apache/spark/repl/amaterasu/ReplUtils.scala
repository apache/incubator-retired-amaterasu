package org.apache.spark.repl.amaterasu

import java.io.{ByteArrayOutputStream, File, PrintWriter}

import io.shinto.amaterasu.common.configuration.ClusterConfig
import org.apache.spark.SparkConf
import org.apache.spark.util.Utils

import scala.tools.nsc.Settings

import scala.tools.nsc.interpreter.IMain

/**
  * Created by roadan on 8/13/16.
  */
object ReplUtils {

  var classServerUri: String = null
  var interperter: IMain = null

  def getOrCreateClassServerUri(outStream: ByteArrayOutputStream, jars: Seq[String], recreate: Boolean = false): String = {
    if (interperter == null || recreate) {
      initInterprater(outStream, jars)
    }
    classServerUri
  }

  def getOrCreateScalaInterperter(outStream: ByteArrayOutputStream, jars: Seq[String], recreate: Boolean = false): IMain = {
    if (interperter == null || recreate) {
      initInterprater(outStream, jars)
    }
    interperter
  }

  private def initInterprater(outStream: ByteArrayOutputStream, jars: Seq[String]) = {

    var result: IMain = null
    var classServerUri: String = null
    val config = new ClusterConfig()
    try {
      //val command = new SparkCommandLine(List())

      val settings = new Settings()

      settings.classpath.append(System.getProperty("java.class.path") + File.pathSeparator +
        "dist/spark-" + config.Webserver.sparkVersion + "/lib/*" + File.pathSeparator +
        jars.mkString(File.pathSeparator))

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
    this.interperter = result
    this.classServerUri = classServerUri
  }

}
