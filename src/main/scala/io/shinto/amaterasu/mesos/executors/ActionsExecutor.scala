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

  var master: String = _
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
      env.workingDir = "s3n://amaterasu/worky/worky"
      env.master = s"local[*]"
      //      env.master = s"mesos://$master:5050"
      log.debug(s"spark env: $env")

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

    log.debug(s"creating SparkContext with master ${env.master}")

    val conf = new SparkConf(true)
      .setMaster(env.master)
      .setAppName(jobId)
      .set("spark.executor.uri", s"http://${sys.env("AMA_NODE")}:8000/spark-1.6.1-2.tgz")
      .set("spark.io.compression.codec", "lzf")
      .set("spark.driver.memory", "512m")
      //.set("spark.submit.deployMode", "cluster")
      .set("spark.mesos.coarse", "true")
      .set("spark.executor.instances", "2")
      .set("spark.cores.max", "5")
      //.set("spark.mesos.mesosExecutor.cores", "1")
      .set("spark.hadoop.validateOutputSpecs", "false")
    // .set("hadoop.home.dir", "/home/hadoop/hadoop")
    //      .set("spark.repl.class.uri", Main.getClass().getName) //TODO: :\ check this
    //      .set("spark.submit.deployMode", "client")
    val sc = new SparkContext(conf)
    val hc = sc.hadoopConfiguration

    if (!sys.env("AWS_ACCESS_KEY_ID").isEmpty &&
        !sys.env("AWS_SECRET_ACCESS_KEY").isEmpty) {

      hc.set("fs.s3n.impl", "org.apache.hadoop.fs.s3native.NativeS3FileSystem")
      hc.set("fs.s3n.awsAccessKeyId", sys.env("AWS_ACCESS_KEY_ID"))
      hc.set("fs.s3n.awsSecretAccessKey", sys.env("AWS_SECRET_ACCESS_KEY"))
    }
    sc

  }

}

object ActionsExecutorLauncher extends Logging {

  def main(args: Array[String]) {
    System.loadLibrary("mesos")
    log.debug("Starting a new ActionExecutor")

    val executor = new ActionsExecutor
    executor.jobId = args(0)
    executor.master = args(1)
    val driver = new MesosExecutorDriver(executor)
    driver.run()
  }

}