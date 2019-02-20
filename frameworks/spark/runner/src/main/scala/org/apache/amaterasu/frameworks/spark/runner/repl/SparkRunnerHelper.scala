/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.amaterasu.frameworks.spark.runner.repl

import java.io.{ByteArrayOutputStream, File, PrintWriter}
import java.nio.file.{Files, Paths}

import org.apache.amaterasu.common.configuration.ClusterConfig
import org.apache.amaterasu.common.execution.actions.Notifier
import org.apache.amaterasu.common.logging.Logging
import org.apache.amaterasu.common.runtime.Environment
import org.apache.amaterasu.common.utils.FileUtils
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scala.tools.nsc.GenericRunnerSettings
import scala.tools.nsc.interpreter.IMain

object SparkRunnerHelper extends Logging {

  private val conf = new SparkConf()
  private val rootDir = conf.get("spark.repl.classdir", System.getProperty("java.io.tmpdir"))
  private val outputDir = Files.createTempDirectory(Paths.get(rootDir), "repl").toFile
  outputDir.deleteOnExit()

  private var sparkSession: SparkSession = _

  var notifier: Notifier = _

  private var interpreter: IMain = _

  def getNode: String = sys.env.get("AMA_NODE") match {
    case None => "127.0.0.1"
    case _ => sys.env("AMA_NODE")
  }

  def getOrCreateScalaInterperter(outStream: ByteArrayOutputStream, jars: Seq[String], recreate: Boolean = false): IMain = {
    if (interpreter == null || recreate) {
      initInterpreter(outStream, jars)
    }
    interpreter
  }

  private def scalaOptionError(msg: String): Unit = {
    notifier.error("", msg)
  }

  private def initInterpreter(outStream: ByteArrayOutputStream, jars: Seq[String]): Unit = {

    var result: IMain = null
    val config = new ClusterConfig()
    try {

      val interpArguments = List(
        "-Yrepl-class-based",
        "-Yrepl-outdir", s"${outputDir.getAbsolutePath}",
        "-classpath", jars.mkString(File.separator)
      )

      val settings = new GenericRunnerSettings(scalaOptionError)
      settings.processArguments(interpArguments, processAll = true)

      settings.classpath.append(System.getProperty("java.class.path") + java.io.File.pathSeparator +
        "spark-" + config.Webserver.sparkVersion + "/jars/*" + java.io.File.pathSeparator +
        jars.mkString(java.io.File.pathSeparator))

      settings.usejavacp.value = true

      val out = new PrintWriter(outStream)
      val interpreter = new AmaSparkILoop(out)
      interpreter.setSettings(settings)

      interpreter.create

      val intp = interpreter.getIntp

      settings.embeddedDefaults(Thread.currentThread().getContextClassLoader)
      intp.setContextClassLoader
      intp.initializeSynchronous

      result = intp
    }
    catch {
      case e: Exception =>
        println(new Predef.String(outStream.toByteArray))

    }

    interpreter = result
  }


  def createSpark(env: Environment,
                  sparkAppName: String,
                  jars: Seq[String],
                  sparkConf: Option[Map[String, Any]],
                  executorEnv: Option[Map[String, Any]],
                  config: ClusterConfig,
                  hostName: String): SparkSession = {

    Thread.currentThread().setContextClassLoader(getClass.getClassLoader)
    val minicondaPkgsPath = "miniconda/pkgs"
    val executorMinicondaDirRef = new File(minicondaPkgsPath)
    val minicondaFiles = if (executorMinicondaDirRef.exists) FileUtils.getAllFiles(executorMinicondaDirRef) else new Array[File](0)
    val pyfiles = minicondaFiles.filter(f => f.getName.endsWith(".py") ||
      f.getName.endsWith(".egg") ||
      f.getName.endsWith(".zip"))

    conf.setAppName(sparkAppName)
      .set("spark.driver.host", hostName)
      .set("spark.submit.deployMode", "client")
      .set("spark.hadoop.validateOutputSpecs", "false")
      .set("spark.logConf", "true")
      .set("spark.submit.pyFiles", pyfiles.mkString(","))


    val master: String = if (env.master.isEmpty) {
      "yarn"
    } else {
      env.master
    }

    config.mode match {

      case "mesos" =>
        conf.set("spark.executor.uri", s"http://$getNode:${config.Webserver.Port}/spark-${config.Webserver.sparkVersion}.tgz")
          .setJars(jars)
          .set("spark.master", env.master)
          .set("spark.home", s"${scala.reflect.io.File(".").toCanonical.toString}/spark-${config.Webserver.sparkVersion}")

      case "yarn" =>
        conf.set("spark.home", config.spark.home)
          // TODO: parameterize those
          .setJars(Seq("executor.jar", "spark-runner.jar", "spark-runtime.jar") ++ jars)
          .set("spark.history.kerberos.keytab", "/etc/security/keytabs/spark.headless.keytab")
          .set("spark.driver.extraLibraryPath", "/usr/hdp/current/hadoop-client/lib/native:/usr/hdp/current/hadoop-client/lib/native/Linux-amd64-64")
          .set("spark.yarn.queue", "default")
          .set("spark.history.kerberos.principal", "none")

          .set("spark.master", master)
          .set("spark.executor.instances", config.spark.opts.getOrElse("executor.instances", "1"))
          .set("spark.yarn.jars", s"spark/jars/*")
          .set("spark.executor.memory", config.spark.opts.getOrElse("executor.memory", "1g"))
          .set("spark.dynamicAllocation.enabled", "false")
          .set("spark.eventLog.enabled", "false")
          .set("spark.history.fs.logDirectory", "hdfs:///spark2-history/")
          .set("hadoop.home.dir", config.YARN.hadoopHomeDir)

      case _ => throw new Exception(s"mode ${config.mode} is not legal.")
    }

    if (config.spark.opts != null && config.spark.opts.nonEmpty) {
      config.spark.opts.foreach(kv => {
        log.info(s"Setting ${kv._1} to ${kv._2} as specified in amaterasu.properties")
        conf.set(kv._1, kv._2)
      })
    }

    // adding the the configurations from spark.yml
    sparkConf match {
      case Some(cnf) => {
        for (c <- cnf) {
          if (c._2.isInstanceOf[String])
            conf.set(c._1, c._2.toString)
        }
      }
      case None =>
    }

    // setting the executor env from spark_exec.yml
    executorEnv match {
      case Some(env) => {
        for (c <- env) {
          if (c._2.isInstanceOf[String])
            conf.setExecutorEnv(c._1, c._2.toString)
        }
      }
      case None =>
    }

    conf.set("spark.repl.class.outputDir", outputDir.getAbsolutePath)

    sparkSession = SparkSession.builder
      .appName(sparkAppName)
      .master(env.master)

      //.enableHiveSupport()
      .config(conf).getOrCreate()

    sparkSession.conf.getAll.foreach(x => log.info(x.toString))

    val hc = sparkSession.sparkContext.hadoopConfiguration

    sys.env.get("AWS_ACCESS_KEY_ID") match {
      case None =>
      case _ =>
        hc.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
        hc.set("fs.s3n.awsAccessKeyId", sys.env("AWS_ACCESS_KEY_ID"))
        hc.set("fs.s3n.awsSecretAccessKey", sys.env("AWS_SECRET_ACCESS_KEY"))
    }

    sparkSession
  }
}
