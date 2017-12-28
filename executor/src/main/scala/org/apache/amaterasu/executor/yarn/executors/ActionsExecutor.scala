package org.apache.amaterasu.executor.yarn.executors

import java.io.ByteArrayOutputStream
import java.net.URLDecoder
import scala.collection.JavaConverters._

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.amaterasu.common.dataobjects.{ExecData, TaskData}
import org.apache.amaterasu.common.logging.Logging
import org.apache.amaterasu.executor.common.executors.ProvidersFactory
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.spark.SparkContext


class ActionsExecutor extends Logging {

  var master: String = _
  var sc: SparkContext = _
  var jobId: String = _
  var actionName: String = _
  var taskData: TaskData = _
  var execData: ExecData = _
  var providersFactory: ProvidersFactory = _

  def execute(): Unit = {
    val runner = providersFactory.getRunner(taskData.groupId, taskData.typeId)
    runner match {
      case Some(r) => {
        try {
          r.executeSource(taskData.src, actionName, taskData.exports.asJava)
          log.info("Completed action")
          System.exit(0)
        } catch  {
          case e:Exception => {
            log.error("Exception in execute source", e)
            System.exit(100)
          }
        }
      }
      case None =>
        log.error("", s"Runner not found for group: ${taskData.groupId}, type ${taskData.typeId}. Please verify the tasks")
        System.exit(101)
    }
  }
}

// launched with args:
//s"'${jobManager.jobId}' '${config.master}' '${actionData.name}' '${URLEncoder.encode(gson.toJson(taskData), "UTF-8")}' '${URLEncoder.encode(gson.toJson(execData), "UTF-8")}' '${actionData.id}-${container.getId.getContainerId}'"
object ActionsExecutorLauncher extends App with Logging {

  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)

  log.info("Starting actions executor")
  val jobId = this.args(0)
  val master = this.args(1)
  val actionName = this.args(2)
  log.info("parsing task data")
  val taskData = mapper.readValue(URLDecoder.decode(this.args(3), "UTF-8"), classOf[TaskData])
  log.info("parsing executor data")
  val execData = mapper.readValue(URLDecoder.decode(this.args(4), "UTF-8"), classOf[ExecData])
  val taskIdAndContainerId =  this.args(5)

  val actionsExecutor: ActionsExecutor = new ActionsExecutor
  actionsExecutor.master = master
  actionsExecutor.actionName = actionName
  actionsExecutor.taskData = taskData
  actionsExecutor.execData = execData

  log.info("Setup executor")
  val baos = new ByteArrayOutputStream()
  val notifier = new YarnNotifier(new YarnConfiguration())

  log.info("Setup notifier")
  actionsExecutor.providersFactory = ProvidersFactory(execData, jobId, baos, notifier, taskIdAndContainerId, propFile = "./amaterasu.properties")
  actionsExecutor.execute()
}
