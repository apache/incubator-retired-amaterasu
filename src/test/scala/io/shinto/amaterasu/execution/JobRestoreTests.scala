package io.shinto.amaterasu.execution

import java.util.concurrent.LinkedBlockingQueue

import io.shinto.amaterasu.dataObjects.ActionData
import io.shinto.amaterasu.enums.ActionStatus

import org.apache.curator.framework.{ CuratorFramework, CuratorFrameworkFactory }
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.curator.test.TestingServer
import org.apache.zookeeper.CreateMode

import org.scalatest.{ BeforeAndAfterEach, Matchers, FlatSpec }

import scala.io.Source

/**
  * Created by roadan on 3/18/16.
  */
class JobRestoreTests extends FlatSpec with Matchers with BeforeAndAfterEach {

  val retryPolicy = new ExponentialBackoffRetry(1000, 3)
  val server = new TestingServer(2184, true)
  var client: CuratorFramework = null

  val jobId = s"job_${System.currentTimeMillis}"
  val maki = Source.fromURL(getClass.getResource("/simple-maki.yaml")).mkString
  val queue = new LinkedBlockingQueue[ActionData]()

  var manager: JobManager = null

  client = CuratorFrameworkFactory.newClient(server.getConnectString, retryPolicy)
  client.start()

  override def beforeEach(): Unit = {

    // creating the jobs znode and storing the source repo and branch
    client.create().withMode(CreateMode.PERSISTENT).forPath(s"/$jobId")
    client.create().withMode(CreateMode.PERSISTENT).forPath(s"/$jobId/src", "".getBytes)
    client.create().withMode(CreateMode.PERSISTENT).forPath(s"/$jobId/branch", "".getBytes)

    manager = JobLoader.createJobManager(maki, jobId, client, 3, queue)

  }

  override def afterEach(): Unit = {

    client.delete().deletingChildrenIfNeeded().forPath(s"/$jobId")

  }

  "a restored job" should "have all queued actions in the executionQueue" in {

    // setting task-0000000002 as queued
    client.setData().forPath(s"/${jobId}/task-0000000002", ActionStatus.queued.toString.getBytes)

    JobLoader.restoreJobState(manager, jobId, client)

    queue.peek.name should be("start")
  }

  "a restored job" should "have all started actions in the executionQueue" in {

    // setting task-0000000002 as queued
    client.setData().forPath(s"/${jobId}/task-0000000002", ActionStatus.started.toString.getBytes)

    JobLoader.restoreJobState(manager, jobId, client)

    queue.peek.name should be("start")
  }
}