package org.apache.spark.repl.amaterasu.runners.spark

import java.io.{ ByteArrayOutputStream, PrintWriter }

import io.shinto.amaterasu.Logging
import io.shinto.amaterasu.configuration.environments.Environment
import io.shinto.amaterasu.execution.AmaContext

import org.apache.spark.rdd.RDD
import org.apache.spark.repl.amaterasu.ReplUtils
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.{ SparkConf, SparkContext }
import org.apache.spark.repl.SparkIMain

import scala.collection.mutable
import scala.io.Source
import scala.tools.nsc.Settings
import scala.tools.nsc.interpreter.Results

class ResHolder(var value: Any)

class SparkScalaRunner extends Logging {

  // This is the amaterasu spark configuration need to rethink the name
  var env: Environment = null
  var jobId: String = null
  var interpreter: SparkIMain = null
  var out: PrintWriter = null
  var outStream: ByteArrayOutputStream = null
  var sc: SparkContext = null

  val settings = new Settings()
  val holder = new ResHolder(null)

  def execute(file: String, actionName: String): Unit = {
    initializeAmaContext(env)
    val source = Source.fromFile(file)
    interpretSources(source, actionName)
    interpreter.close()
  }

  def executeSource(actionSource: String, actionName: String): Unit = {
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

        //if (interpreter.prevRequestList.last.value.exists) {

        val result = interpreter.prevRequestList.last.lineRep.call("$result")

        // dear future me (probably Karel or Tim) this is what we
        // can use
        // intresult: Success, Error, etc
        // result: the actual result (RDD, df, etc.) for caching
        // outStream.toString gives you the error message
        intresult match {
          case Results.Success => {
            log.debug("Results.Success")

            //val resultName = interpreter.prevRequestList.last.value.name.toString
            val resultName = interpreter.prevRequestList.last.termNames.last
            //println(interpreter.prevRequestList.last.value)
            if (result != null) {
              result match {
                case df: DataFrame => {
                  log.debug(s"persisting DataFrame: $resultName")
                  val x = interpreter.interpret(s"""$resultName.write.mode(SaveMode.Overwrite).parquet("${env.workingDir}/$jobId/$actionName/$resultName")""")
                  log.debug(s"DF=> $x")
                  log.debug(outStream.toString)
                  log.debug(s"persisted DataFrame: $resultName")
                }
                case rdd: RDD[_] => {
                  log.debug(s"persisting RDD: $resultName")
                  val x = interpreter.interpret(s"""$resultName.saveAsObjectFile("${env.workingDir}/$jobId/$actionName/$resultName")""")
                  log.debug(s"RDD=> $x")
                  log.debug(outStream.toString)
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
        //}
      }
    }
  }

  def initializeAmaContext(env: Environment): Unit = {
    // setting up some context :)
    val sc = this.sc
    val sqlContext = new SQLContext(sc)
    //sqlContext.setConf("spark.sql.parquet.compression.codec", "snappy")

    sc.getConf.getAll.foreach(println)
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

}

object SparkScalaRunner extends Logging {

  def apply(env: Environment, jobId: String, sparkAppName: String): SparkScalaRunner = {

    val result = new SparkScalaRunner()
    result.env = env
    result.jobId = jobId
    result.outStream = new ByteArrayOutputStream()
    val intp = ReplUtils.creteInterprater(env, jobId, result.outStream)
    result.interpreter = intp._1
    //result.classServerUri = intp._2
    result.sc = createSparkContext(env, sparkAppName, intp._2)

    result
  }

  def createSparkContext(env: Environment, sparkAppName: String, classServerUri: String): SparkContext = {

    log.debug(s"creating SparkContext with master ${env.master}")

    val conf = new SparkConf(true)
      .setMaster(env.master)
      .setAppName(sparkAppName)
      .set("spark.executor.uri", s"http://${sys.env("AMA_NODE")}:8000/spark-1.6.1-2.tgz")
      .set("spark.io.compression.codec", "lzf")
      .set("spark.driver.memory", "512m")
      .set("spark.repl.class.uri", classServerUri)
      //.set("spark.submit.deployMode", "cluster")
      .set("spark.mesos.coarse", "true")
      .set("spark.executor.instances", "2")
      .set("spark.cores.max", "5")
      //.set("spark.mesos.mesosExecutor.cores", "1")
      .set("spark.hadoop.validateOutputSpecs", "false")
    // .set("hadoop.home.dir", "/home/hadoop/hadoop")
    //      .set("spark.submit.deployMode", "client")
    val sc = new SparkContext(conf)
    val hc = sc.hadoopConfiguration

    if (!sys.env("AWS_ACCESS_KEY_ID").isEmpty &&
      !sys.env("AWS_SECRET_ACCESS_KEY").isEmpty) {

      hc.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
      hc.set("fs.s3n.awsAccessKeyId", sys.env("AWS_ACCESS_KEY_ID"))
      hc.set("fs.s3n.awsSecretAccessKey", sys.env("AWS_SECRET_ACCESS_KEY"))
    }
    sc

  }

}
