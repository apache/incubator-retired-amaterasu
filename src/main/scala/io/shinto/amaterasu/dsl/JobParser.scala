package io.shinto.amaterasu.dsl

import java.util.concurrent.BlockingQueue

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.databind.node.ArrayNode
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory

import io.shinto.amaterasu.dataObjects.ActionData
import io.shinto.amaterasu.execution.JobManager
import io.shinto.amaterasu.execution.actions.SequentialAction

import org.apache.curator.framework.CuratorFramework

import scala.io.Source

/**
  * The JobParser class is in charge of parsing the maki.yaml file which
  * describes the workflow of an amaterasu job
  */
object JobParser {

  def loadMakiFile(): String = {

    Source.fromFile("repo/maki.yaml").mkString

  }

  def parse(
    jobId: String,
    maki: String,
    jobsQueue: BlockingQueue[ActionData],
    client: CuratorFramework
  ): JobManager = {

    val mapper = new ObjectMapper(new YAMLFactory())
    val job = mapper.readTree(maki)

    // loading the job details
    val manager = JobManager(jobId, job.path("job-name").asText, jobsQueue, client)

    // iterating the flow list and constructing the job's flow
    val flow = job.path("flow").asInstanceOf[ArrayNode]

    for (i <- 0 to flow.size - 1) {

      val actionData = flow.get(i)
      val action = SequentialAction(
        actionData.path("name").asText,
        actionData.path("src").asText,
        actionData.path("type").asText,
        jobId,
        jobsQueue,
        manager.client
      )

      manager.registerAction(action)

    }

    manager
  }

}