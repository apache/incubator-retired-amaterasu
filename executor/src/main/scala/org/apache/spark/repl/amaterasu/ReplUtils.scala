package org.apache.spark.repl.amaterasu

import java.io.{ByteArrayOutputStream, File, PrintWriter}
import java.lang.reflect.Method

import io.shinto.amaterasu.common.configuration.ClusterConfig
import io.shinto.amaterasu.common.runtime.Environment
import org.apache.spark.util.Utils

import scala.tools.nsc.Settings

//import org.apache.spark.HttpServer
import scala.tools.nsc.interpreter.IMain
//import org.apache.spark.repl.

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

//      val rootDir = conf.getOption("spark.repl.classdir").getOrElse(Utils.getLocalDir(conf))
//      val outputDir = Utils.createTempDir(root = rootDir, namePrefix = "repl")

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

//      try {
//        val classServer: Method = intp.getClass.getMethod("classServerUri")
//        classServerUri = classServer.invoke(intp).asInstanceOf[String]
//      }
//      catch {
//        case e: Any =>
//          println(String.format("Spark method classServerUri not available due to: [%s]", e.getMessage))
//      }

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

//  private httpServer(): HttPServer = {
//
//
//  }
}
