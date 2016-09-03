package io.shinto.amaterasu.execution.actions.runners.spark

import io.shinto.amaterasu.Logging

import scala.io.Source

import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue}

/**
  * Created by roadan on 9/2/16.
  */
class PySparkRunner extends Logging {

  def executeSource(actionSource: String, actionName: String): Unit = {
    val source = Source.fromString(actionSource)
    interpretSources(source, actionName)
  }
}

object PySparkExecutionQueue {

  val queue = new LinkedBlockingQueue[String]()

  def getNext(): String = {

    queue.poll()

  }

  def setForExec(line: String) = {

    queue.put(line)

  }

}
