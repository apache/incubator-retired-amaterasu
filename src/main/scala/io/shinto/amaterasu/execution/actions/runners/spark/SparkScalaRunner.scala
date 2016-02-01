package io.shinto.amaterasu.execution.actions.runners.spark

import java.io.{ ByteArrayOutputStream, File, BufferedReader, PrintWriter }

import io.shinto.amaterasu.configuration.SparkConfig
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.repl.{ SparkILoop, Main }

import scala.io.Source
import scala.tools.nsc.Settings

class SparkScalaRunner {

  // This is the amaterasu spark configuration need to rethink the name
  var config: SparkConfig = null
  var actionName: String = null
  var jobId: String = null
  val settings = new Settings()
  var interpreter: SparkILoop = null

  def execute(
    file: String,
    sc: SparkContext,
    actionName: String
  ): Unit = {

    //    SparkIMain.classServer.start()
    //    interpreter.initializeSpark()
    //    val repl = interpreter.intp
    //
    //    repl.setContextClassLoader()
    //    repl.initializeSynchronous()

    // setting up some context :)
    //val x = repl.interpret("@transient var _contextStore: Map[String, AnyRef] = Map[String, AnyRef]()")
    //    val x = repl.interpret("@transient var _binder = new java.util.HashMap[String, Object]()")
    //    val contextStore = repl.valueOfTerm("_binder")
    //
    //    contextStore + ("sc" -> createSparkContext())

    //    println("::::::::::::::::::::::::::::::::::::")
    //    println(contextStore)
    //    println(x)
    //
    //    for (line <- Source.fromFile(file).getLines()) {
    //
    //      if (!line.isEmpty) {
    //
    //        println(line)
    //
    //        val result = repl.interpret(line)
    //
    //        println("-----------------------------")
    //        println(result)
    //
    //      }
    //    }
    //
    //    Main.classServer.stop()

    //    repl.close()
  }
  //
  //  def createSparkContext(): SparkContext = {
  //
  //    val conf = new SparkConf()
  //      .setMaster(config.master)
  //      .setAppName(s"${jobId}_$actionName")
  //      .set("spark.repl.class.uri", Main.classServer.uri) //TODO: :\ check this
  //
  //    new SparkContext(conf)
  //
  //  }

}

//object SparkScalaRunner {
//
//  def apply(config: SparkConfig,
//            actionName: String,
//            jobId: String): SparkScalaRunner = {
//
//    val result = new SparkScalaRunner()
//    result.config = config
//    result.actionName = actionName
//    result.jobId = jobId
//
//    //TODO: revisit this, not sure it should be in an apply method
//    result.settings.processArguments(List("-Yrepl-class-based",
//      "-Yrepl-outdir", s"./",
//      "-classpath", Main.getAddedJars.mkString(File.pathSeparator)), true)
//
//    result.settings.usejavacp.value = true
//
//    val in: Option[BufferedReader] = null
//    val outStream = new ByteArrayOutputStream()
//    val out: PrintWriter = new PrintWriter(outStream)
//    result.interpreter = new SparkILoop(in, out)
//    result.interpreter.settings = result.settings
//
//    result
//
//  }
//
//
//}
