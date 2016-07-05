package io.shinto.amaterasu.execution.actions.runners.spark

import java.io.{ File, ByteArrayOutputStream, BufferedReader, PrintWriter }

import io.shinto.amaterasu.Logging
import io.shinto.amaterasu.configuration.environments.Environment
import io.shinto.amaterasu.configuration.ClusterConfig
import io.shinto.amaterasu.execution.AmaContext
import io.shinto.amaterasu.mesos.executors.ActionsExecutor

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.{ SparkContext, SparkConf }
import org.apache.spark.repl.Main

import scala.collection.mutable
import scala.io.Source
import scala.tools.nsc.Settings
import scala.tools.nsc.interpreter.{ Results, IMain }

class SparkScalaRunner extends Logging {

  // This is the amaterasu spark configuration need to rethink the name
  var config: ClusterConfig = null
  var jobId: String = null
  val settings = new Settings()
  var interpreter: IMain = null
  var out: PrintWriter = null
  var outStream: ByteArrayOutputStream = null
  var sc: SparkContext = null

  def execute(file: String, actionName: String, env: Environment): Unit = {
    initializeAmaContext(env)
    val source = Source.fromFile(file)
    interpretSources(source, actionName)
    interpreter.close()
  }

  def executeSource(actionSource: String, actionName: String, env: Environment): Unit = {
    initializeAmaContext(env)
    val source = Source.fromString(actionSource)
    interpretSources(source, actionName)
    interpreter.close()
  }

  def interpretSources(source: Source, actionName: String): Unit = {
    for (line <- source.getLines()) {

      if (!line.isEmpty) {

        outStream.reset()
        log.debug(line)

        val intresult = interpreter.interpret(line)

        if (interpreter.prevRequestList.last.value.exists) {

          val result = interpreter.prevRequestList.last.lineRep.call("$result")

          // dear future me (probably Karel or Tim) this is what we
          // can use
          // intresult: Success, Error, etc
          // result: the actual result (RDD, df, etc.) for caching
          // outStream.toString gives you the error message
          intresult match {
            case Results.Success => {
              log.debug("Results.Success")

              val resultName = interpreter.prevRequestList.last.value.name.toString
              println(interpreter.prevRequestList.last.value)
              if (result != null) {
                result match {
                  case df: DataFrame => {
                    interpreter.interpret(s"""AmaContext.saveDataFrame($resultName, "$actionName", "$resultName")""")
                    log.debug(s"persisted DataFrame: $resultName")
                  }
                  case rdd: RDD[_] => {
                    interpreter.interpret(s"""AmaContext.saveRDD($resultName, "$actionName", "$resultName")""")
                    log.debug(s"persisted RDD: $resultName")
                  }
                  case _ => println(result)
                }
              }
            }
            case Results.Error => {
              log.debug("Results.Error")
              println(outStream.toString)
            }
            case Results.Incomplete => {
              log.debug("Results.Incomplete")
              log.debug("|")
            }
          }
        }
      }
    }
  }

  def initializeAmaContext(env: Environment): Unit = {
    // setting up some context :)
    val sc = this.sc
    val sqlContext = new SQLContext(sc)

    interpreter.interpret("import scala.util.control.Exception._")
    interpreter.interpret("import org.apache.spark.{ SparkContext, SparkConf }")
    interpreter.interpret("import org.apache.spark.sql.SQLContext")
    interpreter.interpret("import io.shinto.amaterasu.execution.AmaContext")
    interpreter.interpret("import io.shinto.amaterasu.configuration.environments.Environment")

    // creating a map (_contextStore) to hold the different spark contexts
    // in th REPL and getting a reference to it
    interpreter.interpret("var _contextStore = scala.collection.mutable.Map[String, AnyRef]()")
    val contextStore = interpreter.prevRequestList.last.lineRep.call("$result").asInstanceOf[mutable.Map[String, AnyRef]]
    AmaContext.init(sc, sqlContext, jobId, env)

    // populating the contextStore
    contextStore.put("sc", sc)
    contextStore.put("sqlContext", sqlContext)
    contextStore.put("env", env)
    contextStore.put("ac", AmaContext)

    interpreter.interpret("val sc = _contextStore(\"sc\").asInstanceOf[SparkContext]")
    interpreter.interpret("val sqlContext = _contextStore(\"sqlContext\").asInstanceOf[SQLContext]")
    interpreter.interpret("val env = _contextStore(\"env\").asInstanceOf[Environment]")
    interpreter.interpret("val AmaContext = _contextStore(\"ac\").asInstanceOf[AmaContext]")

    // initializing the AmaContext
    println(s"""AmaContext.init(sc, sqlContext ,"$jobId")""")

  }

  def createSparkContext(): SparkContext = {

    val conf = new SparkConf(true)
      .setMaster(s"local[*]")
      .setAppName(s"$jobId")
      .set("spark.repl.class.uri", Main.getClass().getName) //TODO: :\ check this
    //.set("spark.executor.uri", "http://127.0.0.1:8000/spark-assembly-1.6.0-hadoop2.6.0.jar")
    new SparkContext(conf)
  }
}

object SparkScalaRunner {

  def apply(config: ClusterConfig, jobId: String): SparkScalaRunner = {

    val result = new SparkScalaRunner()
    result.config = config
    result.jobId = jobId

    val interpreter = new IMain()

    //TODO: revisit this, not sure it should be in an apply method
    result.settings.processArguments(List(
      "-Yrepl-class-based",
      "-Yrepl-outdir", s"./",
      "-classpath", interpreter.classLoader.getPackages().mkString(File.pathSeparator)
    ), true)

    println(result.settings.classpath)
    //println(System.getProperty("java.class.path"))
    result.settings.usejavacp.value = true

    val in: Option[BufferedReader] = null
    result.outStream = new ByteArrayOutputStream()
    val out = new PrintWriter(result.outStream)
    result.interpreter = new IMain(result.settings, out)
    result.sc = result.createSparkContext()
    result
  }
}
