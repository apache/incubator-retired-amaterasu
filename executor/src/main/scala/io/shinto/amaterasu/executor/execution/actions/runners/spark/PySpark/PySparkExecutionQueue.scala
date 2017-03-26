package io.shinto.amaterasu.executor.execution.actions.runners.spark.PySpark

import java.util.concurrent.{ TimeUnit, LinkedBlockingQueue }

/**
  * Created by roadan on 10/14/16.
  */
class PySparkExecutionQueue {

  val queue = new LinkedBlockingQueue[(String, String)]()

  def getNext(): (String, String) = {

    // if the queue is idle for an hour it will return null which
    // terminates the python execution, need to revisit
    queue.poll(1, TimeUnit.HOURS)

  }

  def setForExec(line: (String, String)) = {

    queue.put(line)

  }

}
