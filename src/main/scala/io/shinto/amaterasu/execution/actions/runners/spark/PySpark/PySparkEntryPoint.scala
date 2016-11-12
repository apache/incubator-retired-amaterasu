package io.shinto.amaterasu.execution.actions.runners.spark.PySpark

import java.net.ServerSocket

import io.shinto.amaterasu.runtime.{AmaContext, Environment}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql.SQLContext
import py4j.GatewayServer

import scala.collection.concurrent.TrieMap

/**
  * Created by roadan on 10/14/16.
  */
object PySparkEntryPoint {

  private var started = false
  private var queue: PySparkExecutionQueue = null
  private var resultQueues: TrieMap[String, ResultQueue] = null
  private var port: Int = 0
  private var jsc: JavaSparkContext = null

  def getExecutionQueue(): PySparkExecutionQueue = {
    queue
  }

  def getResultQueue(actionName: String): ResultQueue = {
    resultQueues.getOrElseUpdate(actionName, new ResultQueue)
  }


  def getJavaSparkContext(): SparkContext = {
    jsc
  }

  def getSparkConf(): SparkConf = {
    jsc.getConf
  }

  private def generatePort(): Int = {

    val socket = new ServerSocket(0)
    val port = socket.getLocalPort

    socket.close()
    port

  }

  def getPort(): Int = {
    port
  }

  def start(
    sc: SparkContext,
    jobName: String,
    env: Environment
  ) = {

    if (!started) {
      AmaContext.init(sc, new SQLContext(sc), jobName, env)
      started = true
    }

    jsc = new JavaSparkContext(sc)
    queue = new PySparkExecutionQueue()
    resultQueues = new TrieMap[String, ResultQueue]()
    port = generatePort()
    val gatewayServer = new GatewayServer(PySparkEntryPoint, port)

    gatewayServer.start()
  }

}
