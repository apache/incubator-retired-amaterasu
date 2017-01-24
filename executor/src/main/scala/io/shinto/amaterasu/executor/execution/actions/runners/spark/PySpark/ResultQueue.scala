package io.shinto.amaterasu.executor.execution.actions.runners.spark.PySpark

import java.util.concurrent.{ TimeUnit, LinkedBlockingQueue }

/**
  * Created by roadan on 10/17/16.
  */
class ResultQueue {
  val queue = new LinkedBlockingQueue[PySparkResult]()

  def getNext(): PySparkResult = {

    // if the queue is idle for an hour it will return null which
    // terminates the python execution, need to revisit
    queue.poll(10, TimeUnit.MINUTES)

  }

  def put(
    resultType: String,
    action: String,
    statement: String,
    message: String
  ) = {

    val result = new PySparkResult(ResultType.withName(resultType), action, statement, message)
    queue.put(result)
  }
}
