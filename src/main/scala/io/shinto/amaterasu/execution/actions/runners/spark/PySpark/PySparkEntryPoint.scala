//package io.shinto.amaterasu.execution.actions.runners.spark.PySpark
//
//import java.net.ServerSocket
//import java.util
//
//import io.shinto.amaterasu.runtime.{ AmaContext, Environment }
//
//import org.apache.spark.{ SparkConf, SparkContext, SparkEnv }
//import org.apache.spark.api.java.JavaSparkContext
//import org.apache.spark.sql.SQLContext
//import org.apache.spark.api.python
//
//import py4j.GatewayServer
//
//import scala.collection.concurrent.TrieMap
//
///**
//  * Created by roadan on 10/14/16.
//  */
//object PySparkEntryPoint1 {
//
//  private var started = false
//  private var queue: PySparkExecutionQueue = null
//  private var resultQueues: TrieMap[String, ResultQueue] = null
//  private var port: Int = 0
//  private var jsc: JavaSparkContext = null
//  private var sparkEnv: SparkEnv = null
//
//  def getExecutionQueue(): PySparkExecutionQueue = {
//    queue
//  }
//
//  def getResultQueue(actionName: String): ResultQueue = {
//    resultQueues.getOrElseUpdate(actionName, new ResultQueue)
//  }
//
//  def getJavaSparkContext(): ExtendedJavaContext = {
//    SparkEnv.set(sparkEnv)
//    jsc
//  }
//
//  def getSparkConf(): SparkConf = {
//    jsc.getConf
//  }
//
//  private def generatePort(): Int = {
//
//    val socket = new ServerSocket(0)
//    val port = socket.getLocalPort
//
//    socket.close()
//    port
//
//  }
//
//  def getPort(): Int = {
//    port
//  }
//
//  def start(
//    sc: SparkContext,
//    jobName: String,
//    env: Environment,
//    sparkEnv: SparkEnv
//  ) = {
//
//    if (!started) {
//      AmaContext.init(sc, new SQLContext(sc), jobName, env)
//      started = true
//    }
//
//    jsc = new JavaSparkContext(sc)
//    PySparkEntryPoint1.sparkEnv = sparkEnv
//    queue = new PySparkExecutionQueue()
//    resultQueues = new TrieMap[String, ResultQueue]()
//    port = generatePort()
//    val gatewayServer = new GatewayServer(PySparkEntryPoint, port)
//
//    gatewayServer.start()
//  }
//
//  //seems like we have to do this because of the scala/py4j integration
//  implicit class ExtendedJavaContext(jsc: JavaSparkContext) {
//
//    def sc() = {
//      jsc.sc
//    }
//
//    def stop = jsc.stop
//
//  }
//
//}