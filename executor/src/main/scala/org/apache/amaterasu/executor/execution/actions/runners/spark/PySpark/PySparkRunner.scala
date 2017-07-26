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

import java.util

import org.apache.amaterasu.common.execution.actions.Notifier
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
    val source = Source.fromFile(actionSource).getLines().mkString("\n")
    interpretSources(source, actionName, exports)
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
            pypath: String): PySparkRunner = {

    val result = new PySparkRunner

    PySparkEntryPoint.start(spark, jobId, env, SparkEnv.get)
    val port = PySparkEntryPoint.getPort
    val proc = Process(Seq("python", getClass.getResource("/spark_intp.py").getPath, port.toString), None,
      "PYTHONPATH" -> pypath,
      "PYSPARK_PYTHON" -> "/usr/bin/python",
      "PYTHONHASHSEED" -> 0.toString) #> System.out

    proc.run()


    result.notifier = notifier

    result
  }

}
