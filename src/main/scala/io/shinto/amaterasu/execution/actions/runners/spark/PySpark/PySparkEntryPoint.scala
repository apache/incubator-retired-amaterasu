package io.shinto.amaterasu.execution.actions.runners.spark.PySpark

import io.shinto.amaterasu.runtime.{Environment, AmaContext}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
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
    resultQueues.getOrElseUpdate(actionName, new ResultQueue)
  }

  def getAmaContext() = {
    AmaContext
  }

  def start(sc: SparkContext,
            jobName: String,
            env: Environment) = {
    if(AmaContext.env == null)
      AmaContext.init(sc,new SQLContext(sc), jobName, env)

    val gatewayServer = new GatewayServer(PySparkEntryPoint)
    gatewayServer.start()
  }

}
