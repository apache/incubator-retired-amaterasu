package io.shinto.amaterasu.execution.actions.runners.spark

import java.io.{ByteArrayOutputStream, File, BufferedReader, PrintWriter}

import io.shinto.amaterasu.configuration.SparkConfig
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.repl.{SparkIMain}

import scala.io.Source
import scala.tools.nsc.Settings
import scala.tools.nsc.interpreter.IMain


class SparkScalaRunner {

  // This is the amaterasu spark configuration need to rethink the name
  var config: SparkConfig = null
  var actionName: String = null
  var jobId: String = null
  val settings = new Settings()
  var interpreter: IMain = new IMain()

  def execute(file: String,
              sc: SparkContext,
              actionName: String): Unit = {

  // setting up some context :)
  //val x = repl.interpret("@transient var _contextStore: Map[String, AnyRef] = Map[String, AnyRef]()")
    val binderDefinition = interpreter.interpret("@transient var _binder = Map[String, AnyRef]()")
    val contextStore = interpreter.valueOfTerm("_binder").orNull.asInstanceOf[Map[String, AnyRef]]

    contextStore  + ("sc" -> createSparkContext())

    println("::::::::::::::::::::::::::::::::::::")
    println(contextStore)
    println(binderDefinition)

    for (line <- Source.fromFile(file).getLines()) {

      if (!line.isEmpty) {

        println(line)

        val result = interpreter.interpret(line)

        println("-----------------------------")
        println(result)

      }
    }

    interpreter.close()
}

  def createSparkContext(): SparkContext = {

    val conf = new SparkConf()
      .setMaster(config.master)
      .setAppName(s"${jobId}_$actionName")
      .set("spark.repl.class.uri", SparkIMain.getClass().getName) //TODO: :\ check this

    new SparkContext(conf)

  }

}

object SparkScalaRunner {

  def apply(config: SparkConfig,
            actionName: String,
            jobId: String): SparkScalaRunner = {

    val result = new SparkScalaRunner()
    result.config = config
    result.actionName = actionName
    result.jobId = jobId

    val interpreter = new IMain()

    //TODO: revisit this, not sure it should be in an apply method
    result.settings.processArguments(List("-Yrepl-class-based",
      "-Yrepl-outdir", s"./",
      "-classpath", interpreter.getInterpreterClassLoader().getPackages().mkString(File.pathSeparator)), true)

    result.settings.usejavacp.value = true

    val in: Option[BufferedReader] = null
    val outStream = new ByteArrayOutputStream()
    val out: PrintWriter = new PrintWriter(outStream)
    result.interpreter = new IMain(result.settings, out)
    result
  }
}
