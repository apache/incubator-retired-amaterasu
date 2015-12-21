package io.shinto.amaterasu.execution

import java.util.concurrent.{ ConcurrentHashMap, BlockingQueue }
import java.util.{ List => List }

import org.apache.curator.framework.{ CuratorFramework, CuratorFrameworkFactory }
import org.apache.curator.retry.ExponentialBackoffRetry

import scala.collection._
import com.fasterxml.jackson.annotation.JsonProperty

import io.shinto.amaterasu.execution.actions.{ Action, SequentialAction }
import io.shinto.amaterasu.dataObjects.ActionData

import scala.collection.concurrent.TrieMap
import scala.collection.convert.decorateAsScala._

/**
  * The JobManager manages the lifecycle of a job. It queues new actions for execution,
  * tracks the state of actions and is in charge of communication with the underlying
  * cluster management framework (mesos)
  */
class JobManager(
    @JsonProperty("job-name") jobName: String,
    @JsonProperty("flow") jobFlow: List[SequentialAction]
) {

  val flow: List[SequentialAction] = jobFlow
  val name: String = jobName

  private val registeredFlow = new TrieMap[Int, Action]
  private var jobsQueue: BlockingQueue[ActionData] = null
  private val executingJobs: concurrent.Map[String, ActionData] = new ConcurrentHashMap[String, ActionData].asScala

  /**
    * The init method starts the job execution once the job is created from the maki.yaml file
    * it is in charge of creating the internal flow map, setting up ZooKeeper and executing
    * the first action
    * If the job execution is resumed (a job that was stooped) the init method will restore the
    * state of the job from ZooKepper
    * @param jobId the Id of the job
    * @param zkConnection the connection string to ZooKeeper
    */
  def init(jobId: String, zkConnection: String, queue: BlockingQueue[ActionData]): Unit = {

    jobsQueue = queue
    val retryPolicy = new ExponentialBackoffRetry(1000, 3)
    val client = CuratorFrameworkFactory.newClient(zkConnection, retryPolicy)
    client.start

    if (client.checkExists.forPath(s"/$jobId") != null) {
      loadExistingJob(jobId, client)
    }
    else {
      loadNewJob(jobId, client)
    }

  }

  def loadExistingJob(jobId: String, client: CuratorFramework): Unit = {}

  def loadNewJob(jobId: String, client: CuratorFramework): Unit = {

    for (i <- 0 to flow.size - 1) {

    }

  }

  /**
    * getNextActionData returns the data of the next action to be executed if such action
    * exists
    * @return the ActionData of the next action, returns null if no such action exists
    */
  def getNextActionData(): ActionData = {

    val nextAction: ActionData = jobsQueue.poll()

    if (nextAction != null) {
      executingJobs.put(nextAction.id, nextAction)
    }

    nextAction
  }
}

//object JobManager {
//
//  def apply(data: JobData, name: String): JobManager = {
//
//    val manager = new JobManager()
//    manager.jobsQueue = new LinkedBlockingQueue[ActionData]()
//    manager.executingJobs = ConcurrentHashMap[String, ActionData].asScala
//
//    manager
//  }
//
//}