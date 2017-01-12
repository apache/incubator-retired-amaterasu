package io.shinto.amaterasu.execution

import java.util.concurrent.LinkedBlockingQueue

import io.shinto.amaterasu.dataObjects.ActionData
import io.shinto.amaterasu.dsl.JobParser

import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.curator.test.TestingServer
import org.apache.zookeeper.CreateMode
import org.scalatest.{ Matchers, FlatSpec }

import scala.io.Source

class JobParserTests extends FlatSpec with Matchers {

  val retryPolicy = new ExponentialBackoffRetry(1000, 3)
  val server = new TestingServer(2182, true)
  val client = CuratorFrameworkFactory.newClient(server.getConnectString, retryPolicy)
  client.start()

  val jobId = s"job_${System.currentTimeMillis}"
  val yaml = Source.fromURL(getClass.getResource("/simple-maki.yaml")).mkString
  val queue = new LinkedBlockingQueue[ActionData]()

  // this will be performed by the job bootstrapper
  client.create().withMode(CreateMode.PERSISTENT).forPath(s"/$jobId")

  val job = JobParser.parse(jobId, yaml, queue, client, 1)

  "JobParser" should "parse the simple-maki.yaml" in {

    job.name should be("amaterasu-test")

  }

  //TODO: I suspect this test is not indicative, and that order is assured need to verify this
  it should "also have two actions in the right order" in {

    job.registeredActions.size should be(3)

    job.registeredActions.get("0000000000").get.data.name should be("start")
    job.registeredActions.get("0000000001").get.data.name should be("step2")
    job.registeredActions.get("0000000001-error").get.data.name should be("error-action")

  }

}