package org.apache.spark.repl.amaterasu.runners.spark

import java.io.ByteArrayOutputStream
import java.util

import io.shinto.amaterasu.common.execution.actions.Notifier
import io.shinto.amaterasu.common.logging.Logging
import io.shinto.amaterasu.common.runtime.Environment
import io.shinto.amaterasu.executor.runtime.AmaContext
import io.shinto.amaterasu.sdk.AmaterasuRunner
import org.apache.spark.sql.{Dataset, SparkSession}

import scala.collection.mutable
import scala.collection.JavaConverters._
import scala.io.Source
import scala.tools.nsc.interpreter.{IMain, Results}

class ResHolder(var value: Any)

class SparkScalaRunner(var env: Environment,
                       var jobId: String,
                       var interpreter: IMain,
                       var outStream: ByteArrayOutputStream,
                       var spark: SparkSession,
                       var notifier: Notifier,
                       var jars: Seq[String]) extends Logging with AmaterasuRunner {

  private def scalaOptionError(msg: String): Unit = {
    notifier.error("", msg)
  }

  override def getIdentifier = "scala"

  val holder = new ResHolder(null)

  override def executeSource(actionSource: String, actionName: String, exports: util.Map[String, String]): Unit = {
    val source = Source.fromString(actionSource)
    interpretSources(source, actionName, exports.asScala.toMap)
  }

  def interpretSources(source: Source, actionName: String, exports: Map[String, String]): Unit = {

    notifier.info(s"================= started action $actionName =================")

    for (line <- source.getLines()) {

      if (!line.isEmpty) {

        outStream.reset()
        log.debug(line)

        if (line.startsWith("import")) {
          interpreter.interpret(line)
        }
        else {

          val intresult = interpreter.interpret(line)

          val result = interpreter.prevRequestList.last.lineRep.call("$result")

          // dear future me (probably Karel or Tim) this is what we
          // can use
          // intresult: Success, Error, etc
          // result: the actual result (RDD, df, etc.) for caching
          // outStream.toString gives you the error message
          intresult match {
            case Results.Success =>
              log.debug("Results.Success")

              notifier.success(line)

              val resultName = interpreter.prevRequestList.last.termNames.last

              if (exports.contains(resultName.toString)) {

                val format = exports(resultName.toString)

                if (result != null) {

                  result match {
                    case ds: Dataset[_] =>
                      log.debug(s"persisting DataFrame: $resultName")
                      interpreter.interpret(s"""$resultName.write.mode(SaveMode.Overwrite).format("$format").save("${env.workingDir}/$jobId/$actionName/$resultName")""")

                      log.debug(s"persisted DataFrame: $resultName")

                    case _ => println(result)
                  }
                }
              }

            case Results.Error =>
              log.debug("Results.Error")
              val err = outStream.toString
              notifier.error(line, err)
              throw new Exception(err)

            case Results.Incomplete =>
              log.debug("Results.Incomplete")

          }
        }
      }
    }

    notifier.info(s"================= finished action $actionName =================")
  }

  def initializeAmaContext(env: Environment): Unit = {

    // setting up some context :)
    val sc = this.spark.sparkContext
    val sqlContext = this.spark.sqlContext

    interpreter.interpret("import scala.util.control.Exception._")
    interpreter.interpret("import org.apache.spark.{ SparkContext, SparkConf }")
    interpreter.interpret("import org.apache.spark.sql.SQLContext")
    interpreter.interpret("import org.apache.spark.sql.{ Dataset, SparkSession }")
    interpreter.interpret("import org.apache.spark.sql.SaveMode")
    interpreter.interpret("import io.shinto.amaterasu.executor.runtime.AmaContext")
    interpreter.interpret("import io.shinto.amaterasu.common.runtime.Environment")

    // creating a map (_contextStore) to hold the different spark contexts
    // in th REPL and getting a reference to it
    interpreter.interpret("var _contextStore = scala.collection.mutable.Map[String, AnyRef]()")
    val contextStore = interpreter.prevRequestList.last.lineRep.call("$result").asInstanceOf[mutable.Map[String, AnyRef]]
    AmaContext.init(spark, jobId, env)

    // populating the contextStore
    contextStore.put("sc", sc)
    contextStore.put("sqlContext", sqlContext)
    contextStore.put("env", env)
    contextStore.put("spark", spark)
    contextStore.put("ac", AmaContext)

    interpreter.interpret("val sc = _contextStore(\"sc\").asInstanceOf[SparkContext]")
    interpreter.interpret("val sqlContext = _contextStore(\"sqlContext\").asInstanceOf[SQLContext]")
    interpreter.interpret("val env = _contextStore(\"env\").asInstanceOf[Environment]")
    interpreter.interpret("val spark = _contextStore(\"spark\").asInstanceOf[SparkSession]")
    interpreter.interpret("val AmaContext = _contextStore(\"ac\").asInstanceOf[AmaContext]")
    interpreter.interpret("import sqlContext.implicits._")

    // initializing the AmaContext
    println(s"""AmaContext.init(sc, sqlContext ,"$jobId")""")

  }

}

object SparkScalaRunner extends Logging {

  def apply(env: Environment,
            jobId: String,
            spark: SparkSession,
            outStream: ByteArrayOutputStream,
            notifier: Notifier,
            jars: Seq[String]): SparkScalaRunner = {

    new SparkScalaRunner(env, jobId, SparkRunnerHelper.getOrCreateScalaInterperter(outStream, jars), outStream, spark, notifier, jars)

  }

}
