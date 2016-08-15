package org.apache.spark.repl.amaterasu

import java.io.{ PrintWriter, File, ByteArrayOutputStream }
import java.lang.reflect.Method

import io.shinto.amaterasu.configuration.environments.Environment
import org.apache.spark.repl.{ SparkCommandLine, SparkIMain }

/**
  * Created by roadan on 8/13/16.
  */
object ReplUtils {

  def creteInterprater(env: Environment, jobId: String, outStream: ByteArrayOutputStream): (SparkIMain, String) = {

    var result: SparkIMain = null
    var classServerUri: String = null

    try {

      val command =
        new SparkCommandLine(List())

      val settings = command.settings

      settings.classpath.append(System.getProperty("java.class.path") + File.pathSeparator +
        "spark-assembly-1.6.1-hadoop2.4.0.jar" + File.pathSeparator +
        "/home/vagrant/spark-1.6.1-1/conf/")

      settings.usejavacp.value = true

      //val in: Option[BufferedReader] = null
      val out = new PrintWriter(outStream)
      val interpreter = new AmaSparkILoop(out)
      interpreter.setSttings(settings)

      interpreter.create

      val intp = interpreter.getIntp

      try {
        val classServer: Method = intp.getClass.getMethod("classServerUri")
        classServerUri = classServer.invoke(intp).asInstanceOf[String]
      }
      catch {
        case e: Any =>
          println(String.format("Spark method classServerUri not available due to: [%s]", e.getMessage))
      }

      println(classServerUri)

      settings.embeddedDefaults(Thread.currentThread()
        .getContextClassLoader)
      intp.setContextClassLoader
      intp.initializeSynchronous
      //intp.bind("$result", result.holder.getClass.getName, result.holder)

      result = intp
    }
    catch {
      case e: Exception =>
        println("+++++++>" + new Predef.String(outStream.toByteArray))

    }
    (result, classServerUri)
  }
}
