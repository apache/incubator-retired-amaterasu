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
package org.apache.amaterasu.leader.mesos

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import org.apache.amaterasu.common.configuration.ClusterConfig
import org.apache.amaterasu.common.dataobjects.ActionData
import org.apache.amaterasu.common.execution.actions.Notification
import org.apache.amaterasu.common.execution.actions.enums.NotificationLevel
import org.apache.amaterasu.common.execution.actions.enums.NotificationType
import org.apache.amaterasu.common.logging.KLogging
import org.apache.amaterasu.leader.common.execution.JobLoader
import org.apache.amaterasu.leader.common.execution.JobManager
import org.apache.amaterasu.leader.common.utilities.ActiveReportListener
import org.apache.amaterasu.leader.common.utilities.HttpServer
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.log4j.LogManager
import org.apache.mesos.Protos
import org.apache.mesos.Scheduler
import org.apache.mesos.SchedulerDriver
import java.io.File
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.LinkedBlockingQueue

class JobScheduler(private val src: String,
                   private val branch: String,
                   private val username: String,
                   private val password: String,
                   private val env: String,
                   private val resume: Boolean,
                   private val config: ClusterConfig,
                   private val report: String,
                   private val home: String) : Scheduler, KLogging() {

    private var client: CuratorFramework
    private val server = HttpServer()
    private val listener = ActiveReportListener()
    private lateinit var jobManager: JobManager
    private val mapper = ObjectMapper().registerKotlinModule()
    private var reportLevel: NotificationLevel
    private val offersToTaskIds =  ConcurrentHashMap<String, String>()
    private val taskIdsToActions =  ConcurrentHashMap<Protos.TaskID, String>()

    init {
        LogManager.resetConfiguration()
        server.start(config.webserver().Port(), "$home/${config.webserver().Root()}")

        reportLevel = NotificationLevel.valueOf(report.capitalize())
        val retryPolicy = ExponentialBackoffRetry(1000, 3)
        client = CuratorFrameworkFactory.newClient(config.zk(), retryPolicy)
        client.start()
    }

    /**
     * creates a working dir for a job
     * @param jobId: the id of the job (will be used as the jobs directory name as well)
     */
    private fun createJobDir(jobId: String) {
        val jarFile = File(this::class.java.protectionDomain.codeSource.location.path)
        val amaHome = File(jarFile.parent).parent
        val jobDir = "$amaHome/dist/$jobId/"

        val dir = File(jobDir)
        if (!dir.exists()) {
            dir.mkdir()
        }
    }

    override fun registered(driver: SchedulerDriver?, frameworkId: Protos.FrameworkID?, masterInfo: Protos.MasterInfo?) {
        jobManager = if (!resume) {
            JobLoader.loadJob(src,
                    branch,
                    frameworkId!!.value,
                    username,
                    password,
                    client,
                    config.jobs().tasks().attempts(),
                    LinkedBlockingQueue<ActionData>())
        } else {
            JobLoader.reloadJob(frameworkId!!.value,
                    username,
                    password,
                    client,
                    config.jobs().tasks().attempts(),
                    LinkedBlockingQueue<ActionData>())
        }

        jobManager.start()
        createJobDir(jobManager.jobId)
    }

    override fun disconnected(driver: SchedulerDriver?) {}

    override fun reregistered(driver: SchedulerDriver?, masterInfo: Protos.MasterInfo?) {}

    override fun error(driver: SchedulerDriver?, message: String?) {
        log.error(message)
    }

    override fun slaveLost(driver: SchedulerDriver?, slaveId: Protos.SlaveID?) {}

    override fun executorLost(driver: SchedulerDriver?, executorId: Protos.ExecutorID?, slaveId: Protos.SlaveID?, status: Int) {}

    override fun statusUpdate(driver: SchedulerDriver?, status: Protos.TaskStatus?) {
        status?.let {

            val actionName = taskIdsToActions[it.taskId]
            when (it.state) {
                Protos.TaskState.TASK_STARTING -> log.info("Task starting ...")
                Protos.TaskState.TASK_RUNNING -> {
                    jobManager.actionStarted(it.taskId.value)
                    listener.printNotification(Notification("", "created container for $actionName created", NotificationType.Info, NotificationLevel.Execution))
                }
                Protos.TaskState.TASK_FINISHED -> {
                    jobManager.actionComplete(it.taskId.value)
                    listener.printNotification(Notification("", "Container ${it.executorId.value} Complete with task ${it.taskId.value} with success.", NotificationType.Info, NotificationLevel.Execution))
                }
                Protos.TaskState.TASK_FAILED,
                Protos.TaskState.TASK_KILLED,
                Protos.TaskState.TASK_ERROR,
                Protos.TaskState.TASK_LOST -> {
                    jobManager.actionFailed(it.taskId.value, it.message)
                    listener.printNotification(Notification("", "error launching container with ${it.message} in ${it.data.toStringUtf8()}", NotificationType.Error, NotificationLevel.Execution))
                }
                else -> log.warn("WTF? just got unexpected task state: " + it.state)
            }
        }
    }

    override fun frameworkMessage(driver: SchedulerDriver?, executorId: Protos.ExecutorID?, slaveId: Protos.SlaveID?, data: ByteArray?) {

        data?.let {
            val notification: Notification = mapper.readValue(it)

            when (reportLevel) {
                NotificationLevel.Code -> listener.printNotification(notification)
                NotificationLevel.Execution ->
                    if (notification.notLevel != NotificationLevel.Code)
                        listener.printNotification(notification)
            }
        }
    }

    override fun resourceOffers(driver: SchedulerDriver?, offers: MutableList<Protos.Offer>?) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun offerRescinded(driver: SchedulerDriver?, offerId: Protos.OfferID?) {
        offerId?.let{
            val actionId = offersToTaskIds[it.value]
            jobManager.reQueueAction(actionId!!)
        }
    }

}