package io.shinto.amaterasu.leader.execution

import java.util.concurrent.BlockingQueue

import io.shinto.amaterasu.common.dataobjects.ActionData
import io.shinto.amaterasu.common.logging.Logging
import io.shinto.amaterasu.enums.ActionStatus
import io.shinto.amaterasu.leader.dsl.{GitUtil, JobParser}
import org.apache.curator.framework.CuratorFramework
import org.apache.zookeeper.CreateMode

import scala.collection.JavaConverters._

/**
  * Created by roadan on 3/7/16.
  */
object JobLoader extends Logging {

  def loadJob(src: String, branch: String, jobId: String, client: CuratorFramework, attempts: Int, actionsQueue: BlockingQueue[ActionData]): JobManager = {

    // creating the jobs znode and storing the source repo and branch
    client.create().withMode(CreateMode.PERSISTENT).forPath(s"/$jobId")
    client.create().withMode(CreateMode.PERSISTENT).forPath(s"/$jobId/repo", src.getBytes)
    client.create().withMode(CreateMode.PERSISTENT).forPath(s"/$jobId/branch", branch.getBytes)

    val maki: String = loadMaki(src, branch)

    val jobManager: JobManager = createJobManager(maki, jobId, client, attempts, actionsQueue)

    //jobManager.start()
    jobManager

  }

  def createJobManager(maki: String, jobId: String, client: CuratorFramework, attempts: Int, actionsQueue: BlockingQueue[ActionData]): JobManager = {

    val jobManager = JobParser.parse(
      jobId,
      maki,
      actionsQueue,
      client,
      attempts
    )
    jobManager
  }

  def loadMaki(src: String, branch: String): String = {

    // cloning the git repo
    log.debug(s"getting repo: $src, for branch $branch")
    GitUtil.cloneRepo(src, branch)

    // parsing the maki.yaml and creating a JobManager to
    // coordinate the workflow based on the file
    val maki = JobParser.loadMakiFile()
    maki
  }

  def reloadJob(jobId: String, client: CuratorFramework, attempts: Int, actionsQueue: BlockingQueue[ActionData]) = {

    //val jobState = client.getChildren.forPath(s"/$jobId")
    val src = new String(client.getData.forPath(s"/$jobId/repo"))
    val branch = new String(client.getData.forPath(s"/$jobId/branch"))

    val maki: String = loadMaki(src, branch)

    val jobManager: JobManager = createJobManager(maki, jobId, client, attempts, actionsQueue)
    restoreJobState(jobManager, jobId, client)

    jobManager.start()
    jobManager
  }

  def restoreJobState(jobManager: JobManager, jobId: String, client: CuratorFramework): Unit = {

    val tasks = client.getChildren.forPath(s"/$jobId").asScala.toSeq.filter(n => n.startsWith("task"))
    for (task <- tasks) {

      if (client.getData.forPath(s"/$jobId/$task").sameElements(ActionStatus.queued.toString.getBytes) ||
        client.getData.forPath(s"/$jobId/$task").sameElements(ActionStatus.started.toString.getBytes)) {

        jobManager.reQueueAction(task.substring(task.indexOf("task-") + 5))

      }

    }

  }

}