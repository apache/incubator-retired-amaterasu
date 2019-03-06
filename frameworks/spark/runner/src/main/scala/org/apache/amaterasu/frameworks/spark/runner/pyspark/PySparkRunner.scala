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
package org.apache.amaterasu.frameworks.spark.runner.pyspark

import java.io.File
import java.util

import org.apache.amaterasu.common.configuration.ClusterConfig
import org.apache.amaterasu.common.execution.actions.Notifier
import org.apache.amaterasu.common.execution.dependencies.PythonDependencies
import org.apache.amaterasu.common.logging.Logging
import org.apache.amaterasu.common.runtime.Environment
import org.apache.amaterasu.sdk.AmaterasuRunner
import org.apache.spark.SparkEnv
import org.apache.spark.sql.SparkSession

import scala.sys.process.{Process, ProcessLogger}




class PySparkRunner extends Logging with AmaterasuRunner {

  var proc: Process = _
  var notifier: Notifier = _

  override def getIdentifier: String = "pyspark"

  override def executeSource(actionSource: String, actionName: String, exports: util.Map[String, String]): Unit = {
    interpretSources(actionSource, actionName, exports)
  }

  def interpretSources(source: String, actionName: String, exports: util.Map[String, String]): Unit = {

    PySparkEntryPoint.getExecutionQueue.setForExec((source, actionName, exports))
    val resQueue = PySparkEntryPoint.getResultQueue(actionName)

    notifier.info(s"================= Started action $actionName =================")

    var res: PySparkResult = null

    do {
      res = resQueue.getNext()
      res.resultType match {
        case ResultType.success =>
          notifier.success(res.statement)
        case ResultType.error =>
          notifier.error(res.statement, res.message)
          throw new Exception(res.message)
        case ResultType.completion =>
          notifier.info(s"================= finished action $actionName =================")
      }
    } while (res != null && res.resultType != ResultType.completion)
  }

}

object PySparkRunner {

  def collectCondaPackages(): String = {
    val pkgsDirs = new File("./miniconda/pkgs")
    (pkgsDirs.listFiles.filter {
      file => file.getName.endsWith(".tar.bz2")
    }.map {
      file => s"./miniconda/pkgs/${file.getName}"
    }.toBuffer ++ "dist/codegen.py").mkString(",")
  }

  def apply(env: Environment,
            jobId: String,
            notifier: Notifier,
            spark: SparkSession,
            pypath: String,
            pyDeps: PythonDependencies,
            config: ClusterConfig): PySparkRunner = {

    val shellLoger = ProcessLogger(
      (o: String) => println(o),
      (e: String) => println(e)
    )

    //TODO: can we make this less ugly?


    val result = new PySparkRunner

    PySparkEntryPoint.start(spark, jobId, env, SparkEnv.get)
    val port = PySparkEntryPoint.getPort
    var intpPath = ""
    if (env.configuration.contains("cwd")) {
      val cwd = new File(env.configuration("cwd"))
      intpPath = s"${cwd.getAbsolutePath}/spark_intp.py" // This is to support test environment
    } else {
      intpPath = s"spark_intp.py"
    }
    var pysparkPath = ""
    var condaPkgs = ""
    if (pyDeps != null)
      condaPkgs = collectCondaPackages()
    var sparkCmd: Seq[String] = Seq()
    config.mode match {
      case "yarn" =>
        pysparkPath = s"spark/bin/spark-submit"
        sparkCmd = Seq(pysparkPath, "--py-files", condaPkgs, "--master", "yarn", intpPath, port.toString)
        val proc = Process(sparkCmd, None,
          "PYTHONPATH" -> pypath,
          "PYTHONHASHSEED" -> 0.toString)

        proc.run(shellLoger)
      case "mesos" =>
        pysparkPath = config.pysparkPath
        if (pysparkPath.endsWith("spark-submit")) {
          sparkCmd = Seq(pysparkPath, "--py-files", condaPkgs, intpPath, port.toString)
        }
        else {
          sparkCmd = Seq(pysparkPath, intpPath, port.toString)
    }
        var pysparkPython = "/usr/bin/python"

        if (pyDeps != null &&
          pyDeps.packages.nonEmpty) {
          pysparkPython = "./miniconda/bin/python"
        }
        val proc = Process(sparkCmd, None,
      "PYTHONPATH" -> pypath,
      "PYSPARK_PYTHON" -> pysparkPython,
      "PYTHONHASHSEED" -> 0.toString)

        proc.run(shellLoger)
    }

    result.notifier = notifier

    result
  }

}
