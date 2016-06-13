package io.shinto.amaterasu.execution.actions.runners.spark

import java.io.{ ByteArrayOutputStream, File, BufferedReader, PrintWriter }

import io.shinto.amaterasu.configuration.{ ClusterConfig }
import org.apache.spark.sql.SQLContext
import org.apache.spark.{ SparkContext, SparkConf }
import org.apache.spark.repl.{ Main }

import scala.collection.mutable
import scala.io.Source
import scala.tools.nsc.Settings
import scala.tools.nsc.interpreter.{ Results, IMain }
import scala.tools.nsc.interpreter.Results.{ Incomplete, Success }

class SparkScalaRunner {

  // This is the amaterasu spark configuration need to rethink the name
  var config: ClusterConfig = null
  var actionName: String = null
  var jobId: String = null
  val settings = new Settings()
  var interpreter: IMain = null
  var out: PrintWriter = null
  var outStream: ByteArrayOutputStream = null

  def execute(
    file: String,
    sc: SparkContext,
    actionName: String
  ): Unit = {

    // setting up some context :)
    val sc = createSparkContext()
    val sqlContext = new SQLContext(sc)

    interpreter.interpret("import scala.util.control.Exception._")
    interpreter.interpret("import org.apache.spark.{ SparkContext, SparkConf }")
    interpreter.interpret("import org.apache.spark.sql.SQLContext")
    interpreter.interpret("import io.shinto.amaterasu.execution.AmaContext")

    // creating a map (_contextStore) to hold the different spark contexts
    // in th REPL and getting a reference to it
    interpreter.interpret("var _contextStore = scala.collection.mutable.Map[String, AnyRef]()")
    val contextStore = interpreter.prevRequestList.last.lineRep.call("$result").asInstanceOf[mutable.Map[String, AnyRef]]

    // populating the contextStore
    contextStore.put("sc", sc)
    contextStore.put("sqlContext", sqlContext)
    val x = interpreter.interpret("val sc = _contextStore(\"sc\").asInstanceOf[SparkContext]")
    val y = interpreter.interpret("val sqlContext = _contextStore(\"sqlContext\").asInstanceOf[SQLContext]")

    // initializing the AmaContext
    println(s"""AmaContext.init(sc, sqlContext ,"$jobId")""")

    interpreter.interpret(s"""AmaContext.init(sc, sqlContext ,"$jobId")""")
    println(interpreter.prevRequestList.last.value)

    for (line <- Source.fromFile(file).getLines()) {

      if (!line.isEmpty) {

        outStream.reset()
        println("-----------------------------")
        println(line)
        val intresult = interpreter.interpret(line)

        // dear future me (probably Karel or Tim) this is what we
        // can use
        // intresult: Success, Error, etc
        // result: the actual result (RDD, df, etc.) for caching
        // outStream.toString gives you the error message
        intresult match {
          case Results.Success => {
            println("Results.Success")
            //println(interpreter.prevRequestList.last.lineRep.call("$result"))
            println(interpreter.prevRequestList.last.value)
            //            interpreter.prevRequestList.last.value match {
            //
            //            }
          }
          case Results.Error => {
            println("Results.Error")
            println(outStream.toString)
          }
          case Results.Incomplete => {
            println("Results.Incomplete")
            println("|")
          }
        }

      }
    }

    interpreter.close()
  }

  def createSparkContext(): SparkContext = {

    val conf = new SparkConf(true)
      .setMaster(s"local[*]")
      //.setMaster(s"mesos://${config.master}:${config.masterPort}")
      .setAppName(s"${jobId}_$actionName")
      .set("spark.repl.class.uri", Main.getClass().getName) //TODO: :\ check this
    //.set("spark.executor.uri", "<path to spark-1.6.1.tar.gz uploaded above>")
    new SparkContext(conf)
  }
}

object SparkScalaRunner {

  def apply(
    config: ClusterConfig,
    actionName: String,
    jobId: String
  ): SparkScalaRunner = {

    val result = new SparkScalaRunner()
    result.config = config
    result.actionName = actionName
    result.jobId = jobId

    val interpreter = new IMain()

    //TODO: revisit this, not sure it should be in an apply method
    result.settings.processArguments(List(
      "-Yrepl-class-based",
      "-Yrepl-outdir", s"./",
      "-classpath", interpreter.classLoader.getPackages().mkString(File.pathSeparator)
    ), true)

    result.settings.usejavacp.value = true

    val in: Option[BufferedReader] = null
    result.outStream = new ByteArrayOutputStream()
    val out = new PrintWriter(result.outStream)
    result.interpreter = new IMain(result.settings, out)
    result
  }
}
