package io.shinto.amaterasu.mesos.executors

import java.io.{ File, ByteArrayInputStream }

import org.eclipse.aether.util.artifact.JavaScopes
import org.sonatype.aether.artifact.Artifact

import collection.JavaConversions._
import collection.JavaConverters._

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import io.shinto.amaterasu.execution.dependencies.{ Repo, Dependencies }
import io.shinto.amaterasu.Logging

import org.apache.mesos.protobuf.ByteString
import org.apache.mesos.Protos._
import org.apache.mesos.{ MesosExecutorDriver, ExecutorDriver, Executor }

import org.apache.spark.repl.amaterasu.runners.spark.SparkScalaRunner
import org.apache.spark.SparkContext

import org.sonatype.aether.repository.RemoteRepository
import org.sonatype.aether.util.artifact.DefaultArtifact

import com.jcabi.aether.Aether

import scala.collection.mutable
import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.util.{ Success, Failure }

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
  var notifier: MesosNotifier = null

  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)

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
    val data = mapper.readValue(new ByteArrayInputStream(executorInfo.getData.toByteArray), classOf[ExecData])
    val sparkAppName = s"job_${jobId}_executor_${executorInfo.getExecutorId.getValue}"

    notifier = new MesosNotifier(driver)
    notifier.info(s"Executor ${executorInfo.getExecutorId.getValue} registered")

    var jars = Seq[String]()
    if (data.deps != null) {
      jars ++= getDependencies(data.deps)
    }

    sparkScalaRunner = SparkScalaRunner(data.env, jobId, sparkAppName, notifier, jars)
  }

  override def launchTask(driver: ExecutorDriver, taskInfo: TaskInfo): Unit = {

    notifier.info(s"launching task: ${taskInfo.getTaskId.getValue}")
    log.debug(s"launching task: $taskInfo")
    val status = TaskStatus.newBuilder
      .setTaskId(taskInfo.getTaskId)
      .setState(TaskState.TASK_STARTING).build()

    driver.sendStatusUpdate(status)

    val task = Future{
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
      ).toBuffer[Artifact]
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