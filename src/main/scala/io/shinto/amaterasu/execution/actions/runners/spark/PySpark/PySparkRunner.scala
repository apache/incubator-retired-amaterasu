package io.shinto.amaterasu.execution.actions.runners.spark.PySpark

import io.shinto.amaterasu.Logging
import io.shinto.amaterasu.execution.actions.Notifier
import io.shinto.amaterasu.runtime.Environment

import scala.sys.process.Process
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
          notifier.error(res.statement,res.message)
          throw new Exception(res.message)
        case ResultType.completion =>
          notifier.info(s"================= finished action $actionName =================")
      }
    }
    while (res.resultType != ResultType.completion)
  }

}

object PySparkRunner {

  def apply(env: Environment,
            notifier: Notifier): PySparkRunner = {

    val result = new PySparkRunner

    result.notifier = notifier

    PySparkEntryPoint.start()
    val proc = Process(getClass.getResource("/spark_intp.py").getPath)
    result.proc = proc.run()

    result
  }

}
