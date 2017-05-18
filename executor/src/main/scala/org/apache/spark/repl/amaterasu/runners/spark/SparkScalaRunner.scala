package org.apache.spark.repl.amaterasu.runners.spark

import java.io.ByteArrayOutputStream

import io.shinto.amaterasu.common.execution.actions.Notifier
import io.shinto.amaterasu.common.logging.Logging
import io.shinto.amaterasu.common.runtime.Environment
import io.shinto.amaterasu.executor.runtime.AmaContext
import io.shinto.amaterasu.sdk.AmaterasuRunner
import org.apache.spark.rdd.RDD
import org.apache.spark.repl.amaterasu.ReplUtils
import org.apache.spark.sql.{ Dataset, SparkSession}

import scala.collection.mutable
import scala.io.Source
import scala.tools.nsc.Settings
import scala.tools.nsc.interpreter.{IMain, Results}

class ResHolder(var value: Any)

class SparkScalaRunner(var env: Environment,
                       var jobId: String,
                       var interpreter: IMain,
                       var outStream: ByteArrayOutputStream,
                       var spark: SparkSession,
                       var notifier: Notifier) extends Logging with AmaterasuRunner {

  override def getIdentifier = "scala"

  // This is the amaterasu spark configuration need to rethink the name
  val settings = new Settings()
  val holder = new ResHolder(null)

  override def executeSource(actionSource: String, actionName: String): Unit = {
    val source = Source.fromString(actionSource)
    interpretSources(source, actionName)
  }

  def interpretSources(source: Source, actionName: String): Unit = {

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

          //if (interpreter.prevRequestList.last.value.exists) {

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

              //val resultName = interpreter.prevRequestList.last.value.name.toString
              val resultName = interpreter.prevRequestList.last.termNames.last

              if (result != null) {
                //notifier.info(result.getClass.toString)
                result match {
                  case ds: Dataset[_] =>
                    log.debug(s"persisting DataFrame: $resultName")
                    interpreter.interpret(s"""$resultName.write.mode(SaveMode.Overwrite).format("parquet").save("${env.workingDir}/$jobId/$actionName/$resultName")""")
                    notifier.info(outStream.toString)
                    notifier.info(s"""$resultName.write.mode(SaveMode.Overwrite).format("parquet").save("${env.workingDir}/$jobId/$actionName/$resultName")""")
                    log.debug(outStream.toString)
                    log.debug(s"persisted DataFrame: $resultName")

                  case rdd: RDD[_] =>
                    log.debug(s"persisting RDD: $resultName")
                    interpreter.interpret(s"""$resultName.saveAsObjectFile("${env.workingDir}/$jobId/$actionName/$resultName")""")
                    notifier.info(s"${env.workingDir}/$jobId/$actionName/$resultName")
                    log.debug(outStream.toString)
                    log.debug(s"persisted RDD: $resultName")

                  case _ => println(result)
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

    new SparkScalaRunner(env, jobId, ReplUtils.getOrCreateScalaInterperter(outStream, jars), outStream, spark, notifier)

  }

}
