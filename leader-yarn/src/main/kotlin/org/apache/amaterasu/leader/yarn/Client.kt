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

import org.apache.activemq.ActiveMQConnectionFactory
import org.apache.amaterasu.common.configuration.ClusterConfig
import org.apache.amaterasu.leader.common.launcher.AmaOpts
import org.apache.amaterasu.leader.common.execution.frameworks.FrameworkProvidersFactory
import org.apache.amaterasu.leader.common.utilities.ActiveReportListener
import org.apache.curator.framework.CuratorFrameworkFactory
import org.apache.curator.framework.recipes.barriers.DistributedBarrier
import org.apache.curator.retry.ExponentialBackoffRetry
import org.apache.hadoop.fs.*
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.yarn.api.ApplicationConstants
import org.apache.hadoop.yarn.api.records.*
import org.apache.hadoop.yarn.client.api.YarnClient
import org.apache.hadoop.yarn.client.api.YarnClientApplication
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.exceptions.YarnException
import org.apache.hadoop.yarn.util.Apps
import org.apache.hadoop.yarn.util.ConverterUtils
import org.apache.hadoop.yarn.util.Records
import org.apache.log4j.LogManager
import org.slf4j.LoggerFactory

import javax.jms.*
import java.io.File
import java.io.FileInputStream
import java.io.IOException
import java.util.*

import java.lang.System.exit

class Client {
    private val conf = YarnConfiguration()
    private var fs: FileSystem? = null

    @Throws(IOException::class)
    private fun setLocalResourceFromPath(path: Path): LocalResource {

        val stat = fs!!.getFileStatus(path)
        val fileResource = Records.newRecord(LocalResource::class.java)
        fileResource.resource = ConverterUtils.getYarnUrlFromPath(path)
        fileResource.size = stat.len
        fileResource.timestamp = stat.modificationTime
        fileResource.type = LocalResourceType.FILE
        fileResource.visibility = LocalResourceVisibility.PUBLIC
        return fileResource
    }

    @Throws(Exception::class)
    fun run(opts: AmaOpts, args: Array<String>) {

        LogManager.resetConfiguration()
        val config = ClusterConfig()
        config.load(FileInputStream(opts.home + "/amaterasu.properties"))

        // Create yarnClient
        val yarnClient = YarnClient.createYarnClient()
        yarnClient.init(conf)
        yarnClient.start()

        // Create application via yarnClient
        var app: YarnClientApplication? = null
        try {
            app = yarnClient.createApplication()
        } catch (e: YarnException) {
            LOGGER.error("Error initializing yarn application with yarn client.", e)
            exit(1)
        } catch (e: IOException) {
            LOGGER.error("Error initializing yarn application with yarn client.", e)
            exit(2)
        }

        // Setup jars on hdfs
        try {
            fs = FileSystem.get(conf)
        } catch (e: IOException) {
            LOGGER.error("Eror creating HDFS client isntance.", e)
            exit(3)
        }

        val jarPath = Path(config.yarn().hdfsJarsPath())
        val jarPathQualified = fs!!.makeQualified(jarPath)
        val distPath = Path.mergePaths(jarPathQualified, Path("/dist/"))

        val appContext = app!!.applicationSubmissionContext

        var newId = ""
        if (opts.jobId.isEmpty()) {
            newId = "--new-job-id=" + appContext.applicationId.toString() + "-" + UUID.randomUUID().toString()
        }


        val commands = listOf("env AMA_NODE=" + System.getenv("AMA_NODE") +
                " env HADOOP_USER_NAME=" + UserGroupInformation.getCurrentUser().userName +
                " \$JAVA_HOME/bin/java" +
                " -Dscala.usejavacp=false" +
                " -Xmx2G" +
                " org.apache.amaterasu.leader.yarn.ApplicationMaster " +
                joinStrings(args) +
                newId +
                " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" +
                " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr")


        println("===> command ${commands.joinToString("==")}")

        // Set up the container launch context for the application master
        val amContainer = Records.newRecord(ContainerLaunchContext::class.java)
        amContainer.commands = commands

        // Setup local ama folder on hdfs.
        try {

            println("===> $jarPathQualified")
            if (!fs!!.exists(jarPathQualified)) {
                val home = File(opts.home)
                fs!!.mkdirs(jarPathQualified)

                for (f in home.listFiles()) {
                    fs!!.copyFromLocalFile(false, true, Path(f.getAbsolutePath()), jarPathQualified)
                }

                // setup frameworks
                println("===> setting up frameworks")
                val frameworkFactory = FrameworkProvidersFactory.apply(opts.env, config)
                for (group in frameworkFactory.groups()) {
                    println("===> setting up $group")
                    val framework = frameworkFactory.getFramework(group)

                    for (file in framework.groupResources) {
                        println("===> ${file.absolutePath}")
                        if (file.exists())
                            file.let {
                                val target = Path.mergePaths(distPath, Path(it.path))
                                fs!!.copyFromLocalFile(false, true, Path(file.absolutePath), target)
                                println("===> copying $target")
                            }

                    }
                }
            }

        } catch (e: IOException) {
            println("===> error " + e.message)
            LOGGER.error("Error uploading ama folder to HDFS.", e)
            exit(3)
        } catch (ne: NullPointerException) {
            println("===> ne error " + ne.message)
            LOGGER.error("No files in home dir.", ne)
            exit(4)
        }

        // get version of build
        val version = config.version()

        // get local resources pointers that will be set on the master container env
        val leaderJarPath = String.format("/bin/leader-%s-all.jar", version)
        LOGGER.info("Leader Jar path is: {}", leaderJarPath)
        val mergedPath = Path.mergePaths(jarPath, Path(leaderJarPath))

        // System.out.println("===> path: " + jarPathQualified);
        LOGGER.info("Leader merged jar path is: {}", mergedPath)
        var propFile: LocalResource? = null
        var log4jPropFile: LocalResource? = null

        try {
            propFile = setLocalResourceFromPath(Path.mergePaths(jarPath, Path("/amaterasu.properties")))
            log4jPropFile = setLocalResourceFromPath(Path.mergePaths(jarPath, Path("/log4j.properties")))
        } catch (e: IOException) {
            LOGGER.error("Error initializing yarn local resources.", e)
            exit(4)
        }

        // set local resource on master container
        val localResources = HashMap<String, LocalResource>()

        // making the bin folder's content available to the appMaster
        val bin = fs!!.listFiles(Path.mergePaths(jarPath, Path("/bin")), true)

        while (bin.hasNext()) {
            val binFile = bin.next()
            localResources[binFile.path.name] = setLocalResourceFromPath(binFile.path)
        }

        // making the dist folder's content available to the appMaster
        val dist = fs!!.listFiles(Path.mergePaths(jarPath, Path("/dist")), true)

        while (dist.hasNext()) {
            val distFile = dist.next()
            localResources[distFile.path.name] = setLocalResourceFromPath(distFile.path)
        }

        localResources["amaterasu.properties"] = propFile!!
        localResources["log4j.properties"] = log4jPropFile!!
        amContainer.localResources = localResources

        // Setup CLASSPATH for ApplicationMaster
        val appMasterEnv = HashMap<String, String>()
        setupAppMasterEnv(appMasterEnv)
        appMasterEnv["AMA_CONF_PATH"] = String.format("%s/amaterasu.properties", config.YARN().hdfsJarsPath())
        amContainer.environment = appMasterEnv

        // Set up resource type requirements for ApplicationMaster
        val capability = Records.newRecord(Resource::class.java)
        capability.memory = config.YARN().master().memoryMB()
        capability.virtualCores = config.YARN().master().cores()

        // Finally, set-up ApplicationSubmissionContext for the application
        appContext.applicationName = "amaterasu-" + opts.name
        appContext.amContainerSpec = amContainer
        appContext.resource = capability
        appContext.queue = config.YARN().queue()
        appContext.priority = Priority.newInstance(1)

        // Submit application
        val appId = appContext.applicationId
        LOGGER.info("Submitting application {}", appId)
        try {
            yarnClient.submitApplication(appContext)

        } catch (e: YarnException) {
            LOGGER.error("Error submitting application.", e)
            exit(6)
        } catch (e: IOException) {
            LOGGER.error("Error submitting application.", e)
            exit(7)
        }

        val client = CuratorFrameworkFactory.newClient(config.zk(),
                ExponentialBackoffRetry(1000, 3))
        client.start()

        val newJobId = newId.replace("--new-job-id ", "")
        println("===> /$newJobId-report-barrier")
        val reportBarrier = DistributedBarrier(client, "/$newJobId-report-barrier")
        reportBarrier.setBarrier()
        reportBarrier.waitOnBarrier()

        val address = String(client.data.forPath("/$newJobId/broker"))
        println("===> $address")
        setupReportListener(address)

        var appReport: ApplicationReport? = null
        var appState: YarnApplicationState

        do {
            try {
                appReport = yarnClient.getApplicationReport(appId)
            } catch (e: YarnException) {
                LOGGER.error("Error getting application report.", e)
                exit(8)
            } catch (e: IOException) {
                LOGGER.error("Error getting application report.", e)
                exit(9)
            }

            appState = appReport!!.yarnApplicationState
            if (isAppFinished(appState)) {
                exit(0)
                break
            }
            //LOGGER.info("Application not finished ({})", appReport.getProgress());
            try {
                Thread.sleep(100)
            } catch (e: InterruptedException) {
                LOGGER.error("Interrupted while waiting for job completion.", e)
                exit(137)
            }

        } while (!isAppFinished(appState))

        LOGGER.info("Application {} finished with state {}-{} at {}", appId, appState, appReport!!.finalApplicationStatus, appReport.finishTime)
    }

    private fun isAppFinished(appState: YarnApplicationState): Boolean {
        return appState == YarnApplicationState.FINISHED ||
                appState == YarnApplicationState.KILLED ||
                appState == YarnApplicationState.FAILED
    }

    @Throws(JMSException::class)
    private fun setupReportListener(address: String) {

        val cf = ActiveMQConnectionFactory(address)
        val conn = cf.createConnection()
        conn.start()

        val session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE)

        //TODO: move to a const in common
        val destination = session.createTopic("JOB.REPORT")

        val consumer = session.createConsumer(destination)
        consumer.messageListener = ActiveReportListener()

    }

    private fun setupAppMasterEnv(appMasterEnv: Map<String, String>) {
        Apps.addToEnvironment(appMasterEnv,
                ApplicationConstants.Environment.CLASSPATH.name,
                ApplicationConstants.Environment.PWD.`$`() + File.separator + "*", File.pathSeparator)

        for (c in conf.getStrings(
                YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                *YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
            Apps.addToEnvironment(appMasterEnv, ApplicationConstants.Environment.CLASSPATH.name,
                    c.trim { it <= ' ' }, File.pathSeparator)
        }
    }

    companion object {

        private val LOGGER = LoggerFactory.getLogger(Client::class.java)

        @Throws(Exception::class)
        @JvmStatic
        fun main(args: Array<String>) = ClientArgsParser(args).main(args)

        private fun joinStrings(str: Array<String>): String {

            val builder = StringBuilder()
            for (s in str) {
                builder.append(s)
                builder.append(" ")
            }
            return builder.toString()

        }
    }
}