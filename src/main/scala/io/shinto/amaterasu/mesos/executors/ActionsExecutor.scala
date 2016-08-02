package io.shinto.amaterasu.mesos.executors

import org.apache.mesos.protobuf.ByteString
import io.shinto.amaterasu.Logging
import io.shinto.amaterasu.configuration.environments.Environment
import io.shinto.amaterasu.configuration.{ ClusterConfig, SparkConfig }
import io.shinto.amaterasu.execution.actions.runners.spark.SparkScalaRunner
import org.apache.mesos.Protos._
import org.apache.mesos.{ MesosExecutorDriver, ExecutorDriver, Executor }
import org.apache.spark.repl.Main
import org.apache.spark.{ SparkConf, SparkContext }

/**
  * Created by roadan on 1/1/16.
  */
class ActionsExecutor extends Executor with Logging {

  var executorDriver: ExecutorDriver = null
  var sc: SparkContext = null
  var jobId: String = null

  override def shutdown(driver: ExecutorDriver): Unit = {

  }

  override def disconnected(driver: ExecutorDriver): Unit = ???

  override def killTask(driver: ExecutorDriver, taskId: TaskID): Unit = ???

  override def reregistered(driver: ExecutorDriver, slaveInfo: SlaveInfo): Unit = {
    this.executorDriver = driver
  }

  override def error(driver: ExecutorDriver, message: String): Unit = {

    val status = TaskStatus.newBuilder
      .setData(ByteString.copyFromUtf8(message))
      .setState(TaskState.TASK_ERROR).build()

    driver.sendStatusUpdate(status)

  }

  override def frameworkMessage(driver: ExecutorDriver, data: Array[Byte]): Unit = ???

  override def registered(driver: ExecutorDriver, executorInfo: ExecutorInfo, frameworkInfo: FrameworkInfo, slaveInfo: SlaveInfo): Unit = {
    this.executorDriver = driver
  }

  override def launchTask(driver: ExecutorDriver, taskInfo: TaskInfo): Unit = {

    log.debug(s"launching task: $taskInfo")
    val status = TaskStatus.newBuilder
      .setTaskId(taskInfo.getTaskId)
      .setState(TaskState.TASK_RUNNING).build()

    driver.sendStatusUpdate(status)
    val actionSource = taskInfo.getData().toStringUtf8()

    val actionName = "action-" + taskInfo.getTaskId.getValue
    val sparkAppName = s"job_${jobId}_executor_${taskInfo.getExecutor.getExecutorId.getValue}"

    try {
      val env = Environment()
      env.workingDir = "file:///tmp/amaterasu/work1/"
      env.master = "mesos://192.168.33.11:5050"

      if (sc == null)
        sc = createSparkContext(env, sparkAppName)

      sc.getConf.getAll
      val sparkScalaRunner = SparkScalaRunner(env, jobId, sc)
      sparkScalaRunner.executeSource(actionSource, actionName)
      driver.sendStatusUpdate(TaskStatus.newBuilder()
        .setTaskId(taskInfo.getTaskId)
        .setState(TaskState.TASK_FINISHED).build())
    }
    catch {
      case e: Exception => {
        println(s"launching task failed: ${e.getMessage}")

        System.exit(1)
      }
    }
  }

  def createSparkContext(env: Environment, jobId: String): SparkContext = {

    //System.setProperty("hadoop.home.dir", "/home/hadoop/hadoop")
    val conf = new SparkConf(true)
      .setMaster(env.master)
      .setAppName(jobId)
      .set("spark.executor.uri", "http://192.168.33.11:8000/spark-1.6.1-2.tgz")
      .set("spark.io.compression.codec", "lzf")
      .set("spark.submit.deployMode", "cluster")
      .set("spark.mesos.coarse", "true")
      .set("spark.executor.instances", "2")
      .set("spark.cores.max", "5")
      .set("spark.mesos.mesosExecutor.cores", "1")
      .set("spark.hadoop.validateOutputSpecs", "false")
    // .set("hadoop.home.dir", "/home/hadoop/hadoop")
    //      .set("spark.repl.class.uri", Main.getClass().getName) //TODO: :\ check this
    //      .set("spark.submit.deployMode", "client")
    new SparkContext(conf)
  //sc.getConf.getAll.foreach(println)

  }

}

object ActionsExecutorLauncher extends Logging {

  def main(args: Array[String]) {
    System.loadLibrary("mesos")
    log.debug("Starting a new ActionExecutor")

    val executor = new ActionsExecutor
    executor.jobId = args(0)

    val driver = new MesosExecutorDriver(executor)
    driver.run()
  }

}