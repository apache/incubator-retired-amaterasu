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
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.kotlin.readValue
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import org.apache.amaterasu.common.configuration.ClusterConfig
import org.apache.amaterasu.common.configuration.ConfigManager
import org.apache.amaterasu.common.dataobjects.ActionData
import org.apache.amaterasu.common.execution.actions.Notification
import org.apache.amaterasu.common.execution.actions.enums.NotificationLevel
import org.apache.amaterasu.common.execution.actions.enums.NotificationType
import org.apache.amaterasu.common.logging.KLogging
import org.apache.amaterasu.leader.common.execution.JobLoader
import org.apache.amaterasu.leader.common.execution.JobManager
import org.apache.amaterasu.leader.common.execution.frameworks.FrameworkProvidersFactory
import org.apache.amaterasu.leader.common.utilities.ActiveReportListener
import org.apache.amaterasu.leader.common.utilities.DataLoader
import org.apache.amaterasu.leader.common.utilities.HttpServer
import org.apache.amaterasu.sdk.frameworks.FrameworkSetupProvider
import org.apache.amaterasu.sdk.frameworks.RunnerSetupProvider
import org.apache.commons.io.FileUtils
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.log4j.LogManager
import org.apache.mesos.Protos
import org.apache.mesos.Scheduler
import org.apache.mesos.SchedulerDriver
import org.apache.mesos.protobuf.ByteString
import java.io.File
import java.nio.file.Path
import java.nio.file.Paths
import java.util.*
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
    private val offersToTaskIds = ConcurrentHashMap<String, String>()
    private val taskIdsToActions = ConcurrentHashMap<Protos.TaskID, String>()
    private lateinit var frameworkFactory: FrameworkProvidersFactory
    private lateinit var configManager: ConfigManager
    private val yamlMapper = ObjectMapper(YAMLFactory()).registerKotlinModule()

    private val jarFile = File(this::class.java.protectionDomain.codeSource.location.path)
    private val amaDist = File("${File(jarFile.parent).parent}/dist")


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
        offers?.forEach {
            when {
                validateOffer(it) -> synchronized(jobManager) {
                    val actionData = jobManager.nextActionData

                    // if we got a new action to process
                    actionData?.let { ad ->

                        frameworkFactory = FrameworkProvidersFactory(env, config)
                        val items = frameworkFactory.providers.values.flatMap { x -> x.configurationItems }
                        configManager = ConfigManager(env, "repo", items)

                        val taskId = Protos.TaskID.newBuilder().setValue(actionData.id).build()
                        taskIdsToActions[taskId] = actionData.name

                        createTaskConfiguration(ad)
                        listener.printNotification(Notification("", "looking for ${actionData.groupId} provider", NotificationType.Info, NotificationLevel.Execution))
                        val frameworkProvider = frameworkFactory.providers[actionData.groupId]
                        listener.printNotification(Notification("", "found $frameworkProvider provider", NotificationType.Info, NotificationLevel.Execution))

                        val runnerProvider = frameworkProvider!!.getRunnerProvider(actionData.typeId)

                        listener.printNotification(Notification("", "provider ${runnerProvider::class.qualifiedName}", NotificationType.Info, NotificationLevel.Execution))
                        val execData = DataLoader.getExecutorDataBytes(env, config)
                        val executorId = taskId.value + "-" + UUID.randomUUID()

                        val envConf = configManager.getActionConfiguration(ad.name, ad.config)
                        val commandStr = runnerProvider.getCommand(jobManager.jobId, actionData, envConf, executorId, "")
                        listener.printNotification(Notification("", "container command $commandStr", NotificationType.Info, NotificationLevel.Execution))

                        val command = Protos.CommandInfo
                                .newBuilder()
                                .setValue(commandStr)

                        // setting the container's resources
                        val resources = setupContainerResources(frameworkProvider, runnerProvider, actionData)
                        resources.forEach { r ->
                            command.addUris(Protos.CommandInfo.URI.newBuilder()
                                    .setValue(r)
                                    .setExecutable(false)
                                    .setExtract(true)
                                    .build())
                        }

                        // setting the container's executable
                        val executable: Path = getExecutable(runnerProvider, actionData)
                        command.addUris(Protos.CommandInfo.URI.newBuilder()
                                .setValue(toServingUri(executable.toString()))
                                .setExecutable(false)
                                .setExtract(false)
                                .build())

                        // setting the processes environment variables
                        val envVarsList = frameworkProvider.environmentVariables.map { e ->
                            Protos.Environment.Variable.newBuilder()
                                    .setName(e.key)
                                    .setValue(e.value)
                                    .build()
                        }
                        command.setEnvironment(Protos.Environment.newBuilder().addAllVariables(envVarsList))

                        val driverConfiguration = frameworkProvider.getDriverConfiguration(configManager)

                        var actionTask: Protos.TaskInfo = if (runnerProvider.hasExecutor) {
                            val executor = Protos.ExecutorInfo
                                    .newBuilder()
                                    .setData(ByteString.copyFrom(execData))
                                    .setName(taskId.value)
                                    .setExecutorId(Protos.ExecutorID.newBuilder().setValue(executorId))
                                    .setCommand(command)
                                    .build()

                            Protos.TaskInfo.newBuilder()
                                    .setName(taskId.value)
                                    .setTaskId(taskId)
                                    .setExecutor(executor)
                                    .setData(ByteString.copyFrom(DataLoader.getTaskDataBytes(actionData, env)))
                                    .addResources(createScalarResource("cpus", driverConfiguration.cpus.toDouble()))
                                    .addResources(createScalarResource("mem", driverConfiguration.memory.toDouble()))
                                    .addResources(createScalarResource("disk", config.jobs().repoSize().toDouble()))
                                    .setSlaveId(it.slaveId)
                                    .build()
                            //slavesExecutors.put(offer.getSlaveId.getValue, executor)
                        } else {
                            Protos.TaskInfo.newBuilder()
                                    .setName(taskId.value)
                                    .setTaskId(taskId)
                                    .setCommand(command)
                                    .addResources(createScalarResource("cpus", driverConfiguration.cpus.toDouble()))
                                    .addResources(createScalarResource("mem", driverConfiguration.memory.toDouble()))
                                    .addResources(createScalarResource("disk", config.jobs().repoSize().toDouble()))
                                    .setSlaveId(it.slaveId)
                                    .build()
                        }

                        listener.printNotification(Notification("", "requesting container for ${actionData.name}", NotificationType.Info, NotificationLevel.Execution))
                        driver?.launchTasks(Collections.singleton(it.id), listOf(actionTask))

                    } ?: run {
                        if (jobManager.outOfActions) {
                            log.info("framework ${jobManager.jobId} execution finished")

                            val repo = File("repo/")
                            repo.delete()

                            server.stop()
                            driver?.declineOffer(it.id)
                            driver?.stop()
                            System.exit(0)
                        } else {
                            log.info("Declining offer, no sufficient resources")
                            driver!!.declineOffer(it.id)
                        }
                    }
                }

            }
        }
    }

    private fun getExecutable(runnerProvider: RunnerSetupProvider, actionData: ActionData): Path {
        // setting up action executable
        val sourcePath = File(runnerProvider.getActionExecutable(jobManager.jobId, actionData))
        val executable: Path = if (actionData.hasArtifact) {
            val relativePath = amaDist.toPath().root.relativize(sourcePath.toPath())
            relativePath.subpath(amaDist.toPath().nameCount, relativePath.nameCount)
        } else {
            val dest = File("dist/${jobManager.jobId}/$sourcePath")
            FileUtils.copyFile(sourcePath, dest)
            Paths.get(jobManager.jobId, sourcePath.toPath().toString())
        }
        return executable
    }

    override fun offerRescinded(driver: SchedulerDriver?, offerId: Protos.OfferID?) {
        offerId?.let {
            val actionId = offersToTaskIds[it.value]
            jobManager.requeueAction(actionId!!)
        }
    }

    private fun validateOffer(offer: Protos.Offer): Boolean {
        val resources = offer.resourcesList

        return resources.count { it.name == "cpus" && it.scalar.value >= config.jobs().tasks().cpus() } > 0 &&
                resources.count { it.name == "mem" && it.scalar.value >= config.jobs().tasks().mem() } > 0
    }

    private fun createTaskConfiguration(actionData: ActionData) {

        // setting up the configuration files for the container
        val envYaml = configManager.getActionConfigContent(actionData.name, actionData.config)
        writeConfigFile(envYaml, jobManager.jobId, actionData.name, "env.yaml")

        val dataStores = DataLoader.getTaskData(actionData, env).exports
        val dataStoresYaml = yamlMapper.writeValueAsString(dataStores)
        writeConfigFile(dataStoresYaml, jobManager.jobId, actionData.name, "datastores.yaml")

        val datasets = DataLoader.getDatasets(env)
        writeConfigFile(datasets, jobManager.jobId, actionData.name, "datasets.yaml")

        writeConfigFile("jobId: ${jobManager.jobId}\nactionName: ${actionData.name}", jobManager.jobId, actionData.name, "runtime.yaml")

    }

    private fun writeConfigFile(configuration: String, jobId: String, actionName: String, fileName: String) {
        val jarFile = File(this::class.java.protectionDomain.codeSource.location.path)
        val amaHome = File(jarFile.parent).parent
        val envLocation = "$amaHome/dist/$jobId/$actionName/"

        val dir = File(envLocation)
        if (!dir.exists()) {
            dir.mkdirs()
        }

        File("$envLocation/$fileName").writeText(configuration)
    }

    private fun setupContainerResources(framework: FrameworkSetupProvider, runnerProvider: RunnerSetupProvider, actionData: ActionData): List<String> {
        val result = mutableListOf<String>()

        result.addAll(framework.groupResources.map { toServingUri(it.name) })
        result.addAll(runnerProvider.runnerResources.map { toServingUri(it) })

        val actionDeps = runnerProvider.getActionDependencies(jobManager.jobId, actionData).map {
            FileUtils.copyFile(File(it), File("dist/$it"))
            toServingUri(it)
        }
        result.addAll(actionDeps)

        val actionResources = runnerProvider.getActionResources(jobManager.jobId, actionData).map { toServingUri(it) }
        result.addAll(actionResources
        )
        return result
    }

    private fun toServingUri(file: String): String = "http://${System.getenv("AMA_NODE")}:${config.webserver().Port()}/$file"

    private fun createScalarResource(name: String, value: Double): Protos.Resource {
        return Protos.Resource.newBuilder()
                .setName(name)
                .setType(Protos.Value.Type.SCALAR)
                .setScalar(Protos.Value.Scalar.newBuilder().setValue(value)).build()
    }
}