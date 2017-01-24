package org.apache.spark.repl.amaterasu.runners.spark

import java.io.ByteArrayOutputStream

import io.shinto.amaterasu.common.execution.actions.Notifier
import io.shinto.amaterasu.common.logging.Logging
import io.shinto.amaterasu.common.runtime.Environment
import io.shinto.amaterasu.executor.execution.actions.runners.spark.IAmaRunner
import io.shinto.amaterasu.executor.runtime.AmaContext
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.repl.SparkIMain
import org.apache.spark.repl.amaterasu.ReplUtils
import org.apache.spark.sql.{DataFrame, SQLContext}

import scala.collection.mutable
import scala.io.Source
import scala.tools.nsc.Settings
import scala.tools.nsc.interpreter.Results

class ResHolder(var value: Any)

class SparkScalaRunner(
    var env: Environment,
    var jobId: String,
    var interpreter: SparkIMain,
    var outStream: ByteArrayOutputStream,
    var sc: SparkContext,
    var notifier: Notifier
) extends Logging with IAmaRunner {

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
                result match {
                  case df: DataFrame =>
                    log.debug(s"persisting DataFrame: $resultName")
                    interpreter.interpret(s"""$resultName.write.mode(SaveMode.Overwrite).parquet("${env.workingDir}/$jobId/$actionName/$resultName")""")
                    log.debug(outStream.toString)
                    log.debug(s"persisted DataFrame: $resultName")

                  case rdd: RDD[_] =>
                    log.debug(s"persisting RDD: $resultName")
                    interpreter.interpret(s"""$resultName.saveAsObjectFile("${env.workingDir}/$jobId/$actionName/$resultName")""")
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

  override def initializeAmaContext(env: Environment): Unit = {

    // setting up some context :)
    val sc = this.sc
    val sqlContext = new SQLContext(sc)

    interpreter.interpret("import scala.util.control.Exception._")
    interpreter.interpret("import org.apache.spark.{ SparkContext, SparkConf }")
    interpreter.interpret("import org.apache.spark.sql.SQLContext")
    interpreter.interpret("import org.apache.spark.sql.SaveMode")
    interpreter.interpret("import io.shinto.amaterasu.common.runtime.AmaContext")
    interpreter.interpret("import io.shinto.amaterasu.common.runtime.Environment")

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
    interpreter.interpret("import sqlContext.implicits._")

    // initializing the AmaContext
    println(s"""AmaContext.init(sc, sqlContext ,"$jobId")""")

  }

}

object SparkScalaRunner extends Logging {

  def apply(
    env: Environment,
    jobId: String,
    sparkContext: SparkContext,
    outStream: ByteArrayOutputStream,
    notifier: Notifier,
    jars: Seq[String]
  ): SparkScalaRunner = {
/*<<<<<<< pyspark-support

    val result = new SparkScalaRunner()
    result.env = env
    result.jobId = jobId
    result.outStream = new ByteArrayOutputStream()
    result.notifier = notifier

    val intp = ReplUtils.creteInterprater(env, jobId, result.outStream, jars)

    result.interpreter = intp._1

    result.sc = createSparkContext(env, sparkAppName, intp._2, jars)

    result.initializeAmaContext(env)
    result
  }

  def createSparkContext(env: Environment, sparkAppName: String, classServerUri: String, jars: Seq[String]): SparkContext = {

    log.debug(s"creating SparkContext with master ${env.master}")

    val conf = new SparkConf(true)
      .setMaster(env.master)
      .setAppName(sparkAppName)
      .set("spark.executor.uri", s"http://${sys.env("AMA_NODE")}:8000/spark-1.6.1-2.tgz")
      .set("spark.driver.memory", "512m")
      .set("spark.repl.class.uri", classServerUri)
      .set("spark.mesos.coarse", "true")
      .set("spark.executor.instances", "2")
      .set("spark.cores.max", "5")
      .set("spark.hadoop.validateOutputSpecs", "false")
      .setExecutorEnv('PYTHONPATH',
    val sc = new SparkContext(conf)
    for (jar <- jars) {
      sc.addJar(jar) // and this is how my childhood was ruined :(
    }
    val hc = sc.hadoopConfiguration

    if (!sys.env("AWS_ACCESS_KEY_ID").isEmpty &&
      !sys.env("AWS_SECRET_ACCESS_KEY").isEmpty) {

      hc.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
      hc.set("fs.s3n.awsAccessKeyId", sys.env("AWS_ACCESS_KEY_ID"))
      hc.set("fs.s3n.awsSecretAccessKey", sys.env("AWS_SECRET_ACCESS_KEY"))
    }
    sc

=======*/
    new SparkScalaRunner(env, jobId, ReplUtils.getOrCreateScalaInterperter(outStream, jars), outStream, sparkContext, notifier)

  }

}
