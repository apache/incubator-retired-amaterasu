package io.shinto.amaterasu.execution.actions.runners.spark.PySpark

import java.util.concurrent.{ TimeUnit, LinkedBlockingQueue }

/**
  * Created by roadan on 10/17/16.
  */
class ResultQueue {
  val queue = new LinkedBlockingQueue[PySparkResult]()

  def getNext(): PySparkResult = {

    // if the queue is idle for an hour it will return null which
    // terminates the python execution, need to revisit
    queue.poll(1, TimeUnit.HOURS)

  }

  def put(result: PySparkResult) = {

    queue.put(result)

  }
}
