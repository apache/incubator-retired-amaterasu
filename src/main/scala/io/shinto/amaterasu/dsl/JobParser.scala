package io.shinto.amaterasu.dsl

import java.util.concurrent.BlockingQueue
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory

import io.shinto.amaterasu.dataObjects.ActionData
import io.shinto.amaterasu.execution.JobManager
import io.shinto.amaterasu.execution.actions.{Action, SequentialAction}

import org.apache.curator.framework.CuratorFramework

import scala.collection.mutable.ListBuffer
import scala.io.Source

/**
  * The JobParser class is in charge of parsing the maki.yaml file which
  * describes the workflow of an amaterasu job
  */
object JobParser {

  def loadMakiFile(): String = {

    Source.fromFile("repo/maki.yaml").mkString

  }

  def parse(jobId: String,
            maki: String,
            jobsQueue: BlockingQueue[ActionData],
            client: CuratorFramework): JobManager = {

    val mapper = new ObjectMapper(new YAMLFactory())

    val job = mapper.readTree(maki)

    // loading the job details
    val manager = JobManager(jobId, job.path("job-name").asText, jobsQueue, client)

    // iterating the flow list and constructing the job's flow
    val actions = job.path("flow").asInstanceOf[ArrayNode].asScala.toSeq

    //
    parseActions(actions, manager, jobsQueue, null)

    manager
  }

  /**
    * parseActions is a recursive function, for building the workflow of
    * the job
    * God, I miss Clojure
    * @param actions a seq containing the definitions of all the actions
    * @param manager the job manager for the job
    * @param previous the previous action, this is used in order to add the current action
    *                 to the nextActionIds
    */
  def parseActions(actions: Seq[JsonNode],
                   manager: JobManager,
                   jobsQueue: BlockingQueue[ActionData],
                   previous: Action): Unit = {

    if (!actions.isEmpty) {

      val actionData = actions.head

      val action = SequentialAction(
        actionData.path("name").asText,
        actionData.path("src").asText,
        actionData.path("type").asText,
        manager.jobId,
        jobsQueue,
        manager.client
      )

      if (previous != null)
        previous.data.nextActionIds.append(action.actionId)

      manager.registerAction(action)

      parseActions(actions.tail, manager, jobsQueue, action)
    }
  }

}