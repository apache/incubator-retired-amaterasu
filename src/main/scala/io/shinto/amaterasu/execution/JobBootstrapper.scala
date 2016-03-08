package io.shinto.amaterasu.execution

import java.util.concurrent.LinkedBlockingQueue

import io.shinto.amaterasu.dataObjects.ActionData
import io.shinto.amaterasu.dsl.{ JobParser, GitUtil }
import org.apache.curator.framework.CuratorFramework
import org.apache.zookeeper.CreateMode

/**
  * Created by roadan on 3/7/16.
  */
object JobBootstrapper {

  def bootstrapJob(src: String, branch: String, jobId: String, client: CuratorFramework, attempts: Int): JobManager = {

    // creating the jobs znode and storing the source repo and branch
    client.create().withMode(CreateMode.PERSISTENT).forPath(s"/$jobId")
    client.create().withMode(CreateMode.PERSISTENT).forPath(s"/$jobId/src", src.getBytes)
    client.create().withMode(CreateMode.PERSISTENT).forPath(s"/$jobId/branch", branch.getBytes)

    // cloning the git repo
    GitUtil.cloneRepo(src, branch)

    // parsing the maki.yaml and creating a JobManager to
    // coordinate the workflow based on the file
    val maki = JobParser.loadMakiFile()

    val jobManager = JobParser.parse(
      jobId,
      maki,
      new LinkedBlockingQueue[ActionData],
      client,
      attempts
    )

    jobManager.start()
    jobManager
  }
}