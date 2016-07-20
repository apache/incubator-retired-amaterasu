package io.shinto.amaterasu.execution.actions.runners.spark

import java.io.{ File, ByteArrayOutputStream, BufferedReader, PrintWriter }

import io.shinto.amaterasu.Logging
import io.shinto.amaterasu.configuration.environments.Environment
import io.shinto.amaterasu.execution.AmaContext

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.{ SparkContext, SparkConf }
import org.apache.spark.repl.SparkIMain

import scala.collection.mutable
import scala.io.Source
import scala.tools.nsc.Settings
import scala.tools.nsc.interpreter.{ Results, IMain }

class ResHolder(var value: Any)

class SparkScalaRunner extends Logging {

  // This is the amaterasu spark configuration need to rethink the name
  var env: Environment = null
  var jobId: String = null
  var interpreter: IMain = null
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
                  val x = interpreter.interpret(s"""$resultName.write.mode(SaveMode.Overwrite).parquet("${env.workingDir}/ama-$jobId/$actionName/$resultName")""")
                  log.debug(s"DF=> $x")
                  log.debug(outStream.toString)
                  //interpreter.interpret(s"""AmaContext.saveDataFrame($resultName, "$actionName", "$resultName")""")
                  log.debug(s"persisted DataFrame: $resultName")
                }
                case rdd: RDD[_] => {
                  log.debug(s"persisting RDD: $resultName")
                  val x = interpreter.interpret(s"""$resultName.saveAsObjectFile("${env.workingDir}/ama-$jobId/$actionName/$resultName")""")
                  log.debug(s"RDD=> $x")
                  log.debug(outStream.toString)
                  //interpreter.interpret(s"""AmaContext.saveRDD($resultName, "$actionName", "$resultName")""")
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

    interpreter.interpret("val cl = ClassLoader.getSystemClassLoader")
    interpreter.interpret("cl.asInstanceOf[java.net.URLClassLoader].getURLs.foreach(println)")
    // populating the contextStore
    contextStore.put("sc", sc)
    contextStore.put("sqlContext", sqlContext)
    contextStore.put("env", env)
    contextStore.put("ac", AmaContext)

    // fix for a merges issue (http://stackoverflow.com/questions/17265002/hadoop-no-filesystem-for-scheme-file)
    interpreter.interpret("val hadoopConfig = sc.hadoopConfiguration")
    interpreter.interpret("hadoopConfig.set(\"fs.hdfs.impl\", classOf[org.apache.hadoop.hdfs.DistributedFileSystem].getName)")
    interpreter.interpret("hadoopConfig.set(\"fs.file.impl\", classOf[org.apache.hadoop.fs.LocalFileSystem].getName)")
    interpreter.interpret("hadoopConf.set(\"fs.s3.impl\", \"org.apache.hadoop.fs.s3native.NativeS3FileSystem\")")
    //    hadoopConf.set("fs.s3.awsAccessKeyId", myAccessKey)
    //    hadoopConf.set("fs.s3.awsSecretAccessKey", mySecretKey)

    interpreter.interpret("val sc = _contextStore(\"sc\").asInstanceOf[SparkContext]")
    interpreter.interpret("val sqlContext = _contextStore(\"sqlContext\").asInstanceOf[SQLContext]")
    interpreter.interpret("val env = _contextStore(\"env\").asInstanceOf[Environment]")
    interpreter.interpret("val AmaContext = _contextStore(\"ac\").asInstanceOf[AmaContext]")

    // initializing the AmaContext
    println(s"""AmaContext.init(sc, sqlContext ,"$jobId")""")

  }

}

object SparkScalaRunner {

  def apply(env: Environment, jobId: String, sc: SparkContext): SparkScalaRunner = {

    val result = new SparkScalaRunner()
    result.env = env
    result.jobId = jobId

    val interpreter = new IMain()

    interpreter.bind("$result", result.holder.getClass.getName, result.holder)

    //TODO: revisit this, not sure it should be in an apply method
    result.settings.processArguments(List(
      "-Yrepl-class-based",
      "-Yrepl-outdir", s"./",
      "-classpath", System.getProperty("java.class.path") + ":" +
        "spark-assembly-1.6.2-hadoop2.4.0.jar"
    ), true)

    result.settings.classpath.append(System.getProperty("java.class.path") + ":" +
      "spark-assembly-1.6.2-hadoop2.4.0.jar" //+ ":" +
      )
    println("{{{{{}}}}}")
    println(result.settings.classpath)
    //println(System.getProperty("java.class.path"))

    result.settings.usejavacp.value = true

    val in: Option[BufferedReader] = null
    result.outStream = new ByteArrayOutputStream()
    val out = new PrintWriter(result.outStream)
    result.interpreter = new IMain(result.settings, out)
    result.sc = sc
    result
  }
}
