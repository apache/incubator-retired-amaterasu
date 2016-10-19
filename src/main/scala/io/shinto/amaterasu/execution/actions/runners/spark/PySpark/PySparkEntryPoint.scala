package io.shinto.amaterasu.execution.actions.runners.spark.PySpark

import py4j.GatewayServer

import scala.collection.concurrent.TrieMap

/**
  * Created by roadan on 10/14/16.
  */
object PySparkEntryPoint {

  private var queue: PySparkExecutionQueue = null
  private var resultQueues: TrieMap[String, ResultQueue] = null

  def getExecutionQueue(): PySparkExecutionQueue = {
    if (queue == null) {
      queue = new PySparkExecutionQueue()
    }
    queue
  }

  def getResultQueue(actionName: String): ResultQueue = {
    if (resultQueues == null) {
      resultQueues = new TrieMap[String, ResultQueue]()
    }
    resultQueues.getOrElse(actionName, new ResultQueue)
  }

  def start() = {
    val gatewayServer = new GatewayServer(PySparkEntryPoint)
    gatewayServer.start()
  }

}
