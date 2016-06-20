package io.shinto.amaterasu.execution.actions.runners.spark

import java.io.{ ByteArrayOutputStream, File, BufferedReader, PrintWriter }

import io.shinto.amaterasu.configuration.environments.Environment
import io.shinto.amaterasu.configuration.ClusterConfig
import io.shinto.amaterasu.execution.AmaContext

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.{ SparkContext, SparkConf }
import org.apache.spark.repl.Main

import scala.collection.mutable
import scala.io.Source
import scala.reflect.internal.util.ScalaClassLoader.URLClassLoader
import scala.tools.nsc.Settings
import scala.tools.nsc.interpreter.{ SparkIMain, Results, IMain }

class SparkScalaRunner {

  // This is the amaterasu spark configuration need to rethink the name
  var config: ClusterConfig = null
  var actionName: String = null
  var jobId: String = null
  val settings = new Settings()
  var interpreter: SparkIMain = null
  var out: PrintWriter = null
  var outStream: ByteArrayOutputStream = null

  def execute(file: String, sc: SparkContext, actionName: String, env: Environment): Unit = {

    // setting up some context :)
    val sc = createSparkContext()
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

    // populating the contextStore
    contextStore.put("sc", sc)
    contextStore.put("sqlContext", sqlContext)
    contextStore.put("env", env)

    interpreter.interpret("val sc = _contextStore(\"sc\").asInstanceOf[SparkContext]")
    interpreter.interpret("val sqlContext = _contextStore(\"sqlContext\").asInstanceOf[SQLContext]")
    interpreter.interpret("val env = _contextStore(\"env\").asInstanceOf[Environment]")

    // initializing the AmaContext
    println(s"""AmaContext.init(sc, sqlContext ,"$jobId")""")

    interpreter.interpret(s"""AmaContext.init(sc, sqlContext ,"$jobId", env)""")
    println(interpreter.prevRequestList.last.value)

    for (line <- Source.fromFile(file).getLines()) {

      if (!line.isEmpty) {

        outStream.reset()
        println("-----------------------------")
        println(line)

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
              println("Results.Success")

              val resultName = interpreter.prevRequestList.last.value.name.toString
              println(interpreter.prevRequestList.last.value)
              if (result != null) {
                result match {
                  case df: DataFrame => {
                    AmaContext.saveDataFrame(df, actionName, resultName)
                    println(s"persisted DataFrame: $resultName")
                  }
                  case rdd: RDD[_] => {
                    AmaContext.saveRDD(rdd, actionName, resultName)
                    println(s"persisted RDD: $resultName")
                  }
                  case _ => println(result)
                }
              }
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

    val interpreter = new SparkIMain()

    println("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
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
    result.interpreter = new SparkIMain(result.settings, out)
    result
  }
}
