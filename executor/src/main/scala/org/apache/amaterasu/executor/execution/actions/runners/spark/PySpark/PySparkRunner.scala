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
package org.apache.amaterasu.executor.execution.actions.runners.spark.PySpark

import java.io.{PrintWriter, StringWriter}
import java.util

import org.apache.amaterasu.common.execution.actions.Notifier
import org.apache.amaterasu.common.execution.dependencies.{PythonDependencies, PythonPackage}
import org.apache.amaterasu.common.logging.Logging
import org.apache.amaterasu.common.runtime.Environment
import org.apache.amaterasu.sdk.AmaterasuRunner
import org.apache.spark.SparkEnv
import org.apache.spark.sql.SparkSession

import scala.sys.process.Process
import scala.io.Source

/**
  * Created by roadan on 9/2/16.
  */
class PySparkRunner extends AmaterasuRunner with Logging {

  var proc: Process = _
  var notifier: Notifier = _

  override def getIdentifier: String = "pyspark"

  override def executeSource(actionSource: String, actionName: String, exports: util.Map[String, String]): Unit = {
    interpretSources(actionSource, actionName, exports)
  }

  def interpretSources(source: String, actionName: String, exports: util.Map[String, String]): Unit = {

    PySparkEntryPoint.getExecutionQueue.setForExec((source, actionName, exports))
    val resQueue = PySparkEntryPoint.getResultQueue(actionName)

    notifier.info(s"================= started action $actionName =================")

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

  def apply(env: Environment,
            jobId: String,
            notifier: Notifier,
            spark: SparkSession,
            pypath: String,
            pyDeps: PythonDependencies): PySparkRunner = {

    //TODO: can we make this less ugly?
    var pysparkPython = "/usr/bin/python"

    if (pyDeps != null &&
        pyDeps.packages.nonEmpty) {
      loadPythonDependencies(pyDeps, notifier)
      pysparkPython = "miniconda/bin/python"
    }

    val result = new PySparkRunner

    PySparkEntryPoint.start(spark, jobId, env, SparkEnv.get)
    val port = PySparkEntryPoint.getPort
    val proc = Process(Seq("spark-2.1.1-bin-hadoop2.7/bin/spark-submit", "spark_intp.py", port.toString), None,
      "PYTHONPATH" -> pypath,
      "PYSPARK_PYTHON" -> pysparkPython,
      "PYTHONHASHSEED" -> 0.toString) #> System.out

    proc.run()


    result.notifier = notifier

    result
  }

  private def loadPythonDependencies(deps: PythonDependencies, notifier: Notifier): Unit = {
    notifier.info("loading anaconda evn")
    installAnacondaOnNode()
    //notifier.info("loadPythonDependencies #2")
    //    val py4jPackage = PythonPackage("py4j", channel=Option("conda-forge"))
    //    installAnacondaPackage(py4jPackage)
    //notifier.info("loadPythonDependencies #3")
    val codegenPackage = PythonPackage("codegen", channel = Option("auto"))
    installAnacondaPackage(codegenPackage)
    try {
      // notifier.info("loadPythonDependencies #5")
      deps.packages.foreach(pack => {
        pack.index.getOrElse("anaconda").toLowerCase match {
          case "anaconda" => installAnacondaPackage(pack)
          // case "pypi" => installPyPiPackage(pack)
        }
      })
    }
    catch {

      case rte: RuntimeException =>
        val sw = new StringWriter
        rte.printStackTrace(new PrintWriter(sw))
        notifier.error("", s"Failed to activate environment (runtime) - cause: ${rte.getCause}, message: ${rte.getMessage}, Stack: \n${sw.toString}")
      case e: Exception =>
        val sw = new StringWriter
        e.printStackTrace(new PrintWriter(sw))
        notifier.error("", s"Failed to activate environment (other) - type: ${e.getClass.getName}, cause: ${e.getCause}, message: ${e.getMessage}, Stack: \n${sw.toString}")
    }
  }

  private def installAnacondaPackage(pythonPackage: PythonPackage): Unit = {
    //    log.info(s"installAnacondaPackage: $pythonPackage")
    val channel = pythonPackage.channel.getOrElse("anaconda")
    if (channel == "anaconda") {
      Seq("bash", "-c", s"$$PWD/miniconda/bin/python -m conda install -y ${pythonPackage.packageId}") //! shellLoger
    } else {
      Seq("bash", "-c", s"$$PWD/miniconda/bin/python -m conda install -y -c $channel ${pythonPackage.packageId}") //! shellLoger
    }
  }

  private def installAnacondaOnNode(): Unit = {
    //    log.debug(s"Preparing to install Miniconda")
    Seq("bash", "-c", "sh Miniconda2-latest-Linux-x86_64.sh -b -p $PWD/miniconda") //! shellLoger
    Seq("bash", "-c", "$PWD/miniconda/bin/python -m conda install -y conda-build") //! shellLoger
    //  Seq("bash", "-c", "$PWD/miniconda/bin/python -m conda update anaconda-y") ! shellLoger
    Seq("bash", "-c", "ln -s $PWD/spark-2.1.1-bin-hadoop2.7/python/pyspark $PWD/miniconda/pkgs/pyspark") //! shellLoger
  }


}
