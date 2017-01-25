package io.shinto.amaterasu.common.execution

import java.util.concurrent.LinkedBlockingQueue

import io.shinto.amaterasu.common.dataobjects.ActionData
import io.shinto.amaterasu.leader.dsl.JobParser

import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.curator.test.TestingServer
import org.apache.zookeeper.CreateMode

import org.scalatest.{ Matchers, FlatSpec }

import scala.io.Source

class JobExecutionTests extends FlatSpec with Matchers {

  val retryPolicy = new ExponentialBackoffRetry(1000, 3)
  val server = new TestingServer(2183, true)
  val client = CuratorFrameworkFactory.newClient(server.getConnectString, retryPolicy)
  client.start()

  val jobId = s"job_${System.currentTimeMillis}"
  val yaml = Source.fromURL(getClass.getResource("/simple-maki.yml")).mkString
  val queue = new LinkedBlockingQueue[ActionData]()

  // this will be performed by the job bootstraper
  client.create().withMode(CreateMode.PERSISTENT).forPath(s"/$jobId")
  //  client.setData().forPath(s"/$jobId/src",src.getBytes)
  //  client.setData().forPath(s"/$jobId/branch", branch.getBytes)

  val job = JobParser.parse(jobId, yaml, queue, client, 1)

  "a job" should "queue the first action when the JobManager.start method is called " in {

    job.start
    queue.peek.name should be ("start")

    // making sure that the status is reflected in zk
    val actionStatus = client.getData.forPath(s"/${jobId}/task-0000000000")
    new String(actionStatus) should be("queued")

  }

  it should "return the start action when calling getNextAction and dequeue it" in {

    job.getNextActionData.name should be ("start")
    queue.size should be (0)

    // making sure that the status is reflected in zk
    val actionStatus = client.getData.forPath(s"/${jobId}/task-0000000000")
    new String(actionStatus) should be("started")

  }

  it should "be marked as complete when the actionComplete method is called" in {

    job.actionComplete("0000000000")

    // making sure that the status is reflected in zk
    val actionStatus = client.getData.forPath(s"/${jobId}/task-0000000000")
    new String(actionStatus) should be("complete")

  }

  "the next step2 job" should "be queued as a result of the completion" in {

    queue.peek.name should be ("step2")

    // making sure that the status is reflected in zk
    val actionStatus = client.getData.forPath(s"/${jobId}/task-0000000001")
    new String(actionStatus) should be("queued")

  }

  it should "be marked as started when JobManager.getNextActionData is called" in {

    val data = job.getNextActionData

    data.name should be ("step2")

    // making sure that the status is reflected in zk
    val actionStatus = client.getData.forPath(s"/${jobId}/task-0000000001")
    new String(actionStatus) should be("started")
  }

  it should "be marked as failed when JobManager. is called" in {

    job.actionFailed("0000000001", "test failure")
    queue.peek.name should be ("error-action")

    // making sure that the status is reflected in zk
    val actionStatus = client.getData.forPath(s"/${jobId}/task-0000000001-error")
    new String(actionStatus) should be("queued")

    // and returned by getNextActionData
    val data = job.getNextActionData

  }
}
