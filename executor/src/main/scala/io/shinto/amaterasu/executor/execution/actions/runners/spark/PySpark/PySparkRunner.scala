package io.shinto.amaterasu.executor.execution.actions.runners.spark.PySpark

import java.io.OutputStream

import io.shinto.amaterasu.common.execution.actions.Notifier
import io.shinto.amaterasu.common.logging.Logging
import io.shinto.amaterasu.common.runtime.Environment
import org.apache.spark.{SparkContext, SparkEnv}
import org.apache.spark.sql.SQLContext

import scala.sys.process.{BasicIO, Process, ProcessIO}
import scala.io.Source

/**
  * Created by roadan on 9/2/16.
  */
class PySparkRunner extends Logging {

  var proc: Process = null
  var notifier: Notifier = null

  def executeSource(sourcePath: String, actionName: String): Unit = {
    val source = Source.fromFile(sourcePath).getLines().mkString("\n")
    interpretSources(source, actionName)
  }

  def interpretSources(source: String, actionName: String) = {

    PySparkEntryPoint.getExecutionQueue().setForExec((source, actionName))
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
            sc: SparkContext,
            pypath: String): PySparkRunner = {

    val result = new PySparkRunner

    PySparkEntryPoint.start(sc, jobId, env, SparkEnv.get)
    val port = PySparkEntryPoint.getPort()
    val proc = Process(Seq("python", getClass.getResource("/spark_intp.py").getPath, port.toString), None,
      "PYTHONPATH" -> pypath,
      "PYSPARK_PYTHON" -> "/usr/bin/python",
      "PYTHONHASHSEED" -> 0.toString) #> System.out

    proc.run()


    result.notifier = notifier

    result
  }

}
