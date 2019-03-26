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

import org.apache.activemq.broker.BrokerService
import org.apache.amaterasu.common.configuration.ClusterConfig
import org.apache.amaterasu.common.dataobjects.ActionData
import org.apache.amaterasu.common.logging.KLogging
import org.apache.amaterasu.leader.common.execution.JobLoader
import org.apache.amaterasu.leader.common.execution.JobManager
import org.apache.amaterasu.leader.common.execution.frameworks.FrameworkProvidersFactory
import org.apache.amaterasu.leader.common.launcher.AmaOpts
import org.apache.amaterasu.sdk.frameworks.FrameworkSetupProvider
import org.apache.amaterasu.sdk.frameworks.RunnerSetupProvider
import org.apache.curator.framework.CuratorFramework
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.framework.recipes.barriers.DistributedBarrier
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.yarn.api.records.*
import org.apache.hadoop.yarn.client.api.AMRMClient
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync
import org.apache.hadoop.yarn.client.api.async.NMClientAsync
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.util.ConverterUtils
import org.apache.hadoop.yarn.util.Records
import org.apache.zookeeper.CreateMode
import java.io.File
import java.io.FileInputStream
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.LinkedBlockingQueue

import kotlinx.coroutines.*
import org.apache.hadoop.io.DataOutputBuffer
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.yarn.exceptions.YarnException
import org.apache.hadoop.yarn.security.AMRMTokenIdentifier
import java.io.IOException
import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentHashMap
import kotlin.concurrent.thread

class ApplicationMaster : KLogging(), AMRMClientAsync.CallbackHandler {


    lateinit var address: String

    val broker: BrokerService = BrokerService()
    private val conf = YarnConfiguration()

    private val nmClient: NMClientAsync = NMClientAsyncImpl(YarnNMCallbackHandler())
    private val actionsBuffer = ConcurrentLinkedQueue<ActionData>()
    private val completedContainersAndTaskIds = ConcurrentHashMap<Long, String>()
    private val containersIdsToTask = ConcurrentHashMap<Long, ActionData>()

    private lateinit var propPath: String
    private lateinit var props: FileInputStream
    private lateinit var jobManager: JobManager
    private lateinit var client: CuratorFramework
    private lateinit var env: String
    private lateinit var rmClient: AMRMClientAsync<AMRMClient.ContainerRequest>
    private lateinit var frameworkFactory: FrameworkProvidersFactory
    private lateinit var config: ClusterConfig
    private lateinit var fs: FileSystem


    fun execute(opts: AmaOpts) {

        propPath = System.getenv("PWD") + "/amaterasu.properties"
        props = FileInputStream(File(propPath))

        // no need for hdfs double check (nod to Aaron Rodgers)
        // jars on HDFS should have been verified by the YARN client
        config = ClusterConfig.apply(props)
        fs = FileSystem.get(conf)

        initJob(opts)

        // now that the job was initiated, the curator client is Started and we can
        // register the broker's address
        client.create().withMode(CreateMode.PERSISTENT).forPath("/${jobManager.jobId}/broker")
        client.setData().forPath("/${jobManager.jobId}/broker", address.toByteArray())

        // once the broker is registered, we can remove the barrier so clients can connect
        log.info("/${jobManager.jobId}-report-barrier")
        val barrier = DistributedBarrier(client, "/${jobManager.jobId}-report-barrier")
        barrier.removeBarrier()

        // Initialize clients to ResourceManager and NodeManagers
        nmClient.init(conf)
        nmClient.start()

        rmClient = startRMClient()

        val registrationResponse = rmClient.registerApplicationMaster("", 0, "")
        val maxMem = registrationResponse.maximumResourceCapability.memory
        val maxVCores = registrationResponse.maximumResourceCapability.virtualCores

        while (!jobManager.outOfActions) {
            val capability = Records.newRecord(Resource::class.java)

            val actionData = jobManager.nextActionData
            if (actionData != null) {

                val frameworkProvider = frameworkFactory.getFramework(actionData.groupId)
                val driverConfiguration = frameworkProvider.driverConfiguration

                var mem: Int = driverConfiguration.memory
                mem = Math.min(mem, maxMem)
                capability.memory = mem

                var cpu = driverConfiguration.cpus
                cpu = Math.min(cpu, maxVCores)
                capability.virtualCores = cpu

                requestContainer(actionData, capability)

            }
        }

        log.info("Finished requesting containers")
        readLine()
    }

    private fun initJob(opts: AmaOpts) {

        this.env = opts.env
        frameworkFactory = FrameworkProvidersFactory.apply(env, config)

        try {
            val retryPolicy = ExponentialBackoffRetry(1000, 3)
            client = CuratorFrameworkFactory.newClient(config.zk(), retryPolicy)
            client.start()
        } catch (e: Exception) {
            log.error("Error connecting to zookeeper", e)
            throw e
        }

        val zkPath = client.checkExists().forPath("/${opts.newJobId}")

        log.info("zkPath is $zkPath")
        if (zkPath != null) {
            log.info("resuming job" + opts.newJobId)
            jobManager = JobLoader.reloadJob(
                    opts.newJobId,
                    client,
                    config.Jobs().tasks().attempts(),
                    LinkedBlockingQueue<ActionData>())

        } else {
            log.info("new job is being created")
            try {

                jobManager = JobLoader.loadJob(
                        opts.repo,
                        opts.branch,
                        opts.newJobId,
                        client,
                        config.Jobs().tasks().attempts(),
                        LinkedBlockingQueue<ActionData>())
            } catch (e: Exception) {
                log.error("Error creating JobManager.", e)
                throw e
            }

        }

        jobManager.start()
        log.info("Started jobManager")
    }

    override fun onContainersAllocated(containers: MutableList<Container>?) = runBlocking {
        containers?.let {
            for (container in it) {

                log.info("container ${container.id} allocated")
                if (actionsBuffer.isNotEmpty()) {
                    val actionData = actionsBuffer.poll()
                    val cd = async {
                        log.info("container ${container.id} allocated")

                        val framework = frameworkFactory.getFramework(actionData.groupId)
                        val runnerProvider = framework.getRunnerProvider(actionData.typeId)
                        val ctx = Records.newRecord(ContainerLaunchContext::class.java)
                        val commands: List<String> = listOf(runnerProvider.getCommand(jobManager.jobId, actionData, env, "${actionData.id}-${container.id.containerId}", address))

                        log.info("container command ${commands.joinToString(prefix = " ", postfix = " ")}")
                        ctx.commands = commands
                        ctx.tokens = allTokens()
                        ctx.localResources = setupContainerResources(framework, runnerProvider)
                        ctx.environment = framework.environmentVariables

                        nmClient.startContainerAsync(container, ctx)

                        jobManager.actionStarted(actionData.id)
                        containersIdsToTask.put(container.id.containerId, actionData)
                        log.info("launching container succeeded: ${container.id.containerId}; task: ${actionData.id}")
                    }


                }
            }
        }
    }!!

    private fun allTokens(): ByteBuffer {
        // creating the credentials for container execution
        val credentials = UserGroupInformation.getCurrentUser().credentials
        val dob = DataOutputBuffer()
        credentials.writeTokenStorageToStream(dob)

        // removing the AM->RM token so that containers cannot access it.
        val iter = credentials.allTokens.iterator()
        log.info("Executing with tokens:")
        for (token in iter) {
            log.info(token.toString())
            if (token.kind == AMRMTokenIdentifier.KIND_NAME) iter.remove()
        }
        return ByteBuffer.wrap(dob.data, 0, dob.length)
    }

    /**
     * Creates the map of resources to be copied into the container
     * @framework The frameworkSetupProvider for the action
     * @runnerProvider the actions runner provider
     */
    private fun setupContainerResources(framework: FrameworkSetupProvider, runnerProvider: RunnerSetupProvider): Map<String, LocalResource> {

        val yarnJarPath = Path(config.yarn().hdfsJarsPath())

        // Getting framework (group) resources
        val result = framework.groupResources.map { it.path to createLocalResourceFromPath(Path.mergePaths(yarnJarPath, createDistPath(it.path))) }.toMap().toMutableMap()

        // Getting runner resources
        result.putAll(runnerProvider.runnerResources.map { it to createLocalResourceFromPath(Path.mergePaths(yarnJarPath, createDistPath(it))) }.toMap())

        // Adding the Amaterasu configuration files
        result["amaterasu.properties"] = createLocalResourceFromPath(Path.mergePaths(yarnJarPath, Path("/amaterasu.properties")))
        result["log4j.properties"] = createLocalResourceFromPath(Path.mergePaths(yarnJarPath, Path("/log4j.properties")))

        result.forEach { x -> println("local resource ${x.key} with value ${x.value}") }
        return result.map { x -> x.key.removePrefix("/") to x.value }.toMap()
    }

    private fun createDistPath(path: String): Path = Path("/dist/$path")

    private fun startRMClient(): AMRMClientAsync<AMRMClient.ContainerRequest> {
        val client = AMRMClientAsync.createAMRMClientAsync<AMRMClient.ContainerRequest>(1000, this)
        client.init(conf)
        client.start()
        return client
    }

    private fun createLocalResourceFromPath(path: Path): LocalResource {

        val stat = fs.getFileStatus(path)
        val fileResource = Records.newRecord(LocalResource::class.java)

        fileResource.shouldBeUploadedToSharedCache = true
        fileResource.visibility = LocalResourceVisibility.PUBLIC
        fileResource.resource = ConverterUtils.getYarnUrlFromPath(path)
        fileResource.size = stat.len
        fileResource.timestamp = stat.modificationTime
        fileResource.type = LocalResourceType.FILE
        fileResource.visibility = LocalResourceVisibility.PUBLIC
        return fileResource

    }

    private fun requestContainer(actionData: ActionData, capability: Resource) {

        actionsBuffer.add(actionData)
        log.info("About to ask container for action ${actionData.id} with mem ${capability.memory} and cores ${capability.virtualCores}. Action buffer size is: ${actionsBuffer.size}")

        // we have an action to schedule, let's request a container
        val priority: Priority = Records.newRecord(Priority::class.java)
        priority.priority = 1
        val containerReq = AMRMClient.ContainerRequest(capability, null, null, priority)
        rmClient.addContainerRequest(containerReq)
        log.info("Asked container for action ${actionData.id}")

    }

    override fun onNodesUpdated(updatedNodes: MutableList<NodeReport>?) {
        log.info("Nodes change. Nothing to report.")
    }

    override fun onShutdownRequest() {
        log.error("Shutdown requested.")
        stopApplication(FinalApplicationStatus.KILLED, "Shutdown requested")
    }

    override fun getProgress(): Float {
        return jobManager.registeredActions.size.toFloat() / completedContainersAndTaskIds.size
    }

    override fun onError(e: Throwable?) {
        log.error("Error on AM", e)
        stopApplication(FinalApplicationStatus.FAILED, "Error on AM")
    }

    override fun onContainersCompleted(statuses: MutableList<ContainerStatus>?) {
        for (status in statuses!!) {
            if (status.state == ContainerState.COMPLETE) {

                val containerId = status.containerId.containerId
                val task = containersIdsToTask[containerId]
                rmClient.releaseAssignedContainer(status.containerId)

                val taskId = task!!.id
                if (status.exitStatus == 0) {

                    //completedContainersAndTaskIds.put(containerId, task.id)
                    jobManager.actionComplete(taskId)
                    log.info("Container $containerId Complete with task $taskId with success.")
                } else {
                    // TODO: Check the getDiagnostics value and see if appropriate
                    jobManager.actionFailed(taskId, status.diagnostics)
                    log.warn("Container $containerId Complete with task $taskId with Failed status code (${status.exitStatus})")
                }
            }
        }

        if (jobManager.outOfActions) {
            log.info("Finished all tasks successfully! Wow!")
            jobManager.actionsCount()
            stopApplication(FinalApplicationStatus.SUCCEEDED, "SUCCESS")
        } else {
            log.info("jobManager.registeredActions.size: ${jobManager.registeredActions.size}; completedContainersAndTaskIds.size: ${completedContainersAndTaskIds.size}")
        }
    }

    fun stopApplication(finalApplicationStatus: FinalApplicationStatus, appMessage: String) {

        try {
            rmClient.unregisterApplicationMaster(finalApplicationStatus, appMessage, null)
        } catch (ex: YarnException) {

            log.error("Failed to unregister application", ex)
        } catch (e: IOException) {
            log.error("Failed to unregister application", e)
        }
        rmClient.stop()
        nmClient.stop()
    }

    companion object {
        @JvmStatic
        fun main(args: Array<String>) = AppMasterArgsParser().main(args)

    }
}
