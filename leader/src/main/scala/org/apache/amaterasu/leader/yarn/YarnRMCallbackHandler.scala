/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.amaterasu.leader.yarn

import java.util
import java.util.Collections
import java.util.concurrent.ConcurrentHashMap

import com.google.gson.Gson
import org.apache.amaterasu.common.configuration.ClusterConfig
import org.apache.amaterasu.common.logging.Logging
import org.apache.amaterasu.leader.execution.JobManager
import org.apache.amaterasu.leader.utilities.DataLoader
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api.async.{AMRMClientAsync, NMClientAsync}
import org.apache.hadoop.yarn.util.Records

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.concurrent
import scala.concurrent.Future
import scala.util.{Failure, Success}
import scala.concurrent._
import ExecutionContext.Implicits.global

class YarnRMCallbackHandler(nmClient: NMClientAsync,
                            jobManager: JobManager,
                            env: String,
                            awsEnv: String,
                            config: ClusterConfig,
                            executorJar: LocalResource) extends AMRMClientAsync.CallbackHandler with Logging {


  val gson:Gson = new Gson()
  private val containersIdsToTaskIds: concurrent.Map[Long, String] = new ConcurrentHashMap[Long, String].asScala
  private val completedContainersAndTaskIds: concurrent.Map[Long, String] = new ConcurrentHashMap[Long, String].asScala
  private val failedTasksCounter: concurrent.Map[String, Int] = new ConcurrentHashMap[String, Int].asScala


  override def onError(e: Throwable): Unit = {
    println(s"ERROR: ${e.getMessage}")
  }

  override def onShutdownRequest(): Unit = {
    println("Shutdown requested")
  }

  val MAX_ATTEMPTS_PER_TASK = 3

  override def onContainersCompleted(statuses: util.List[ContainerStatus]): Unit = {
    for (status <- statuses.asScala) {
      if (status.getState == ContainerState.COMPLETE) {
        val containerId = status.getContainerId.getContainerId
        val taskId = containersIdsToTaskIds(containerId)
        if (status.getExitStatus == 0) {
          completedContainersAndTaskIds.put(containerId, taskId)
          log.info(s"Container $containerId completed with task $taskId with success.")
        } else {
          log.warn(s"Container $containerId completed with task $taskId with failed status code (${status.getExitStatus}.")
          val failedTries = failedTasksCounter.getOrElse(taskId, 0)
          if (failedTries < MAX_ATTEMPTS_PER_TASK) {
            // TODO: notify and ask for a new container
            log.info("Retrying task")
          } else {
            log.error(s"Already tried task $taskId $MAX_ATTEMPTS_PER_TASK times. Time to say Bye-Bye.")
            // TODO: die already
          }
        }
      }
    }
    if (getProgress == 1F) {
      log.info("Finished all tasks successfully! Wow!")
    }
  }

  override def getProgress: Float = {
    jobManager.registeredActions.size.toFloat / completedContainersAndTaskIds.size
  }

  override def onNodesUpdated(updatedNodes: util.List[NodeReport]): Unit = {
  }

  override def onContainersAllocated(containers: util.List[Container]): Unit = {
    log.info("containers allocated")
    for (container <- containers.asScala) { // Launch container by create ContainerLaunchContext
      val containerTask = Future[String] {

        val actionData = jobManager.getNextActionData
        val taskData = DataLoader.getTaskData(actionData, env)
        val execData = DataLoader.getExecutorData(env, config)

        val ctx = Records.newRecord(classOf[ContainerLaunchContext])
        val command = s"""$awsEnv env AMA_NODE=${sys.env("AMA_NODE")}
                         | env SPARK_EXECUTOR_URI=http://${sys.env("AMA_NODE")}:${config.Webserver.Port}/dist/spark-${config.Webserver.sparkVersion}.tgz
                         | java -cp executor-0.2.0-all.jar:spark-${config.Webserver.sparkVersion}/lib/*
                         | -Dscala.usejavacp=true
                         | -Djava.library.path=/usr/lib org.apache.amaterasu.executor.yarn.executors.ActionsExecutorLauncher
                         | ${jobManager.jobId} ${config.master} ${actionData.name} ${gson.toJson(taskData)} ${gson.toJson(execData)}""".stripMargin
        ctx.setCommands(Collections.singletonList(command))

        ctx.setLocalResources(Map[String, LocalResource] (
          "executor.jar" -> executorJar
        ))

        nmClient.startContainerAsync(container, ctx)
        actionData.id
      }

      containerTask onComplete {
        case Failure(t) => {
          println(s"launching container failed: ${t.getMessage}")
        }

        case Success(actionDataId) => {
          containersIdsToTaskIds.put(container.getId.getContainerId, actionDataId)
          println(s"launching container succeeded: ${container.getId}")
        }
      }
    }
  }
}
