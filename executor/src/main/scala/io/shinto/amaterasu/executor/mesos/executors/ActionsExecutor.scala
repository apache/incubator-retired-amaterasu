package io.shinto.amaterasu.executor.mesos.executors

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, File}

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.jcabi.aether.Aether
import io.shinto.amaterasu.common.dataobjects.{ExecData, TaskData}
import io.shinto.amaterasu.executor.execution.actions.runners.spark.PySpark.PySparkRunner
import io.shinto.amaterasu.common.execution.dependencies.Dependencies
import io.shinto.amaterasu.common.logging.Logging
import io.shinto.amaterasu.executor.execution.actions.runners.spark.SparkRunnerHelper
import org.apache.mesos.Protos._
import org.apache.mesos.protobuf.ByteString
import org.apache.mesos.{Executor, ExecutorDriver, MesosExecutorDriver}
import org.apache.spark.SparkContext
import org.apache.spark.repl.amaterasu.ReplUtils
import org.apache.spark.repl.amaterasu.runners.spark.SparkScalaRunner
import org.eclipse.aether.util.artifact.JavaScopes
import org.sonatype.aether.repository.RemoteRepository
import org.sonatype.aether.util.artifact.DefaultArtifact

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.util.{Failure, Success}

/**
  * Created by roadan on 1/1/16.
  */
class ActionsExecutor extends Executor with Logging {

  var master: String = _
  var executorDriver: ExecutorDriver = null
  var sc: SparkContext = null
  var jobId: String = null
  var actionName: String = null
  var sparkScalaRunner: SparkScalaRunner = null
  var pySparkRunner: PySparkRunner = null
  var notifier: MesosNotifier = null

  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)

  override def shutdown(driver: ExecutorDriver) = {

  }

  override def killTask(driver: ExecutorDriver, taskId: TaskID) = ???

  override def disconnected(driver: ExecutorDriver) = ???

  override def reregistered(driver: ExecutorDriver, slaveInfo: SlaveInfo) = {
    this.executorDriver = driver
  }

  override def error(driver: ExecutorDriver, message: String) = {

    val status = TaskStatus.newBuilder
      .setData(ByteString.copyFromUtf8(message))
      .setState(TaskState.TASK_ERROR).build()

    driver.sendStatusUpdate(status)

  }

  override def frameworkMessage(driver: ExecutorDriver, data: Array[Byte]) = ???

  override def registered(driver: ExecutorDriver, executorInfo: ExecutorInfo, frameworkInfo: FrameworkInfo, slaveInfo: SlaveInfo) = {

    this.executorDriver = driver
    val data = mapper.readValue(new ByteArrayInputStream(executorInfo.getData.toByteArray), classOf[ExecData])
    val sparkAppName = s"job_${jobId}_executor_${executorInfo.getExecutorId.getValue}"

    notifier = new MesosNotifier(driver)
    notifier.info(s"Executor ${executorInfo.getExecutorId.getValue} registered")

    var jars = Seq[String]()
    if (data.deps != null) {
      jars ++= getDependencies(data.deps)
    }

    val outStream = new ByteArrayOutputStream()

    val classServerUri = ReplUtils.getOrCreateClassServerUri(outStream, jars)
    log.debug(s"creating SparkContext with master ${data.env.master}")
    val sparkContext = SparkRunnerHelper.createSparkContext(data.env, sparkAppName, classServerUri, jars)

    sparkScalaRunner = SparkScalaRunner(data.env, jobId, sparkContext, outStream, notifier, jars)
    sparkScalaRunner.initializeAmaContext(data.env)
  }

  override def launchTask(driver: ExecutorDriver, taskInfo: TaskInfo) = {

    notifier.info(s"launching task: ${taskInfo.getTaskId.getValue}")
    log.debug(s"launching task: $taskInfo")
    val status = TaskStatus.newBuilder
      .setTaskId(taskInfo.getTaskId)
      .setState(TaskState.TASK_STARTING).build()

    driver.sendStatusUpdate(status)

    val task = Future {

      val taskData = mapper.readValue(new ByteArrayInputStream(taskInfo.getData.toByteArray), classOf[TaskData])

      val actionSource = taskData.src

      val status = TaskStatus.newBuilder
        .setTaskId(taskInfo.getTaskId)
        .setState(TaskState.TASK_RUNNING).build()

      driver.sendStatusUpdate(status)

      sparkScalaRunner.executeSource(actionSource, actionName)

    }

    task onComplete {
      case Failure(t) => {
        println(s"launching task failed: ${t.getMessage}")
        System.exit(1)
      }
      case Success(ts) => {

        driver.sendStatusUpdate(TaskStatus.newBuilder()
          .setTaskId(taskInfo.getTaskId)
          .setState(TaskState.TASK_FINISHED).build())
        notifier.info(s"complete task: ${taskInfo.getTaskId.getValue}")
      }
    }

  }

  private def getDependencies(deps: Dependencies): Seq[String] = {

    // adding a local repo because Aether needs one
    val repo = new File(System.getProperty("java.io.tmpdir"), "ama-repo")

    val remotes = deps.repos.map(r =>
      new RemoteRepository(
        r.id,
        r.`type`,
        r.url
      )).toList.asJava

    val aether = new Aether(remotes, repo)

    deps.artifacts.flatMap(a => {
      aether.resolve(
        new DefaultArtifact(a.groupId, a.artifactId, "", "jar", a.version),
        JavaScopes.RUNTIME
      ).map(a => a) // .toBuffer[Artifact]
    }).map(x => x.getFile.getAbsolutePath)

  }
}

object ActionsExecutorLauncher extends Logging {

  def main(args: Array[String]) {
    System.loadLibrary("mesos")
    log.debug("Starting a new ActionExecutor")

    val executor = new ActionsExecutor
    executor.jobId = args(0)
    executor.master = args(1)
    executor.actionName = args(2)

    val driver = new MesosExecutorDriver(executor)
    driver.run()
  }

}