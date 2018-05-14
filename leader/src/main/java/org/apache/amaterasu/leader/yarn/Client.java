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
package org.apache.amaterasu.leader.yarn;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.amaterasu.common.configuration.ClusterConfig;
import org.apache.amaterasu.leader.execution.frameworks.FrameworkProvidersFactory;
import org.apache.amaterasu.leader.utilities.ActiveReportListener;
import org.apache.amaterasu.sdk.frameworks.FrameworkSetupProvider;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.barriers.DistributedBarrier;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Apps;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.hadoop.yarn.util.Records;
import org.apache.log4j.LogManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.jms.*;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;

import static java.lang.System.exit;

public class Client {

    private final static Logger LOGGER = LoggerFactory.getLogger(Client.class);
    private final Configuration conf = new YarnConfiguration();
    private FileSystem fs;

    private LocalResource setLocalResourceFromPath(Path path) throws IOException {

        FileStatus stat = fs.getFileStatus(path);
        LocalResource fileResource = Records.newRecord(LocalResource.class);
        fileResource.setResource(ConverterUtils.getYarnUrlFromPath(path));
        fileResource.setSize(stat.getLen());
        fileResource.setTimestamp(stat.getModificationTime());
        fileResource.setType(LocalResourceType.FILE);
        fileResource.setVisibility(LocalResourceVisibility.PUBLIC);
        return fileResource;
    }

    private void run(JobOpts opts, String[] args) throws Exception {

        LogManager.resetConfiguration();
        ClusterConfig config = new ClusterConfig();
        config.load(new FileInputStream(opts.home + "/amaterasu.properties"));

        // Create yarnClient
        YarnClient yarnClient = YarnClient.createYarnClient();
        yarnClient.init(conf);
        yarnClient.start();

        // Create application via yarnClient
        YarnClientApplication app = null;
        try {
            app = yarnClient.createApplication();
        } catch (YarnException e) {
            LOGGER.error("Error initializing yarn application with yarn client.", e);
            exit(1);
        } catch (IOException e) {
            LOGGER.error("Error initializing yarn application with yarn client.", e);
            exit(2);
        }

        // Setup jars on hdfs
        try {
            fs = FileSystem.get(conf);
        } catch (IOException e) {
            LOGGER.error("Eror creating HDFS client isntance.", e);
            exit(3);
        }
        Path jarPath = new Path(config.YARN().hdfsJarsPath());
        Path jarPathQualified = fs.makeQualified(jarPath);

        ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();

        String newId = "";
        if (opts.jobId == null) {
            newId = "--new-job-id " + appContext.getApplicationId().toString() + "-" + UUID.randomUUID().toString();
        }


        List<String> commands = Collections.singletonList(
                "env AMA_NODE=" + System.getenv("AMA_NODE") + " " +
                        "$JAVA_HOME/bin/java" +
                        " -Dscala.usejavacp=false" +
                        " -Xmx1G" +
                        " org.apache.amaterasu.leader.yarn.ApplicationMaster " +
                        joinStrings(args) +
                        newId +
                        " 1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" +
                        " 2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr"
        );


        // Set up the container launch context for the application master
        ContainerLaunchContext amContainer = Records.newRecord(ContainerLaunchContext.class);
        amContainer.setCommands(commands);

        // Setup local ama folder on hdfs.
        try {

            if (!fs.exists(jarPathQualified)) {
                File home = new File(opts.home);
                fs.mkdirs(jarPathQualified);

                for (File f : home.listFiles()) {
                    fs.copyFromLocalFile(false, true, new Path(f.getAbsolutePath()), jarPathQualified);
                }

                // setup frameworks
                FrameworkProvidersFactory frameworkFactory = FrameworkProvidersFactory.apply(opts.env, config);
                for (String group : frameworkFactory.groups()) {
                    System.out.println("===> setting up " + group);
                    FrameworkSetupProvider framework = frameworkFactory.getFramework(group);

                    //creating a group folder
                    Path frameworkPath = Path.mergePaths(jarPathQualified, new Path("/" + framework.getGroupIdentifier()));
                    System.out.println("===> " + frameworkPath.toString());

                    fs.mkdirs(frameworkPath);
                    for (File file : framework.getGroupResources()) {
                        if (file.exists())
                            fs.copyFromLocalFile(false, true, new Path(file.getAbsolutePath()), frameworkPath);
                    }
                }
            }
        } catch (IOException e) {
            System.out.println("===>" + e.getMessage());
            LOGGER.error("Error uploading ama folder to HDFS.", e);
            exit(3);
        } catch (NullPointerException ne) {
            System.out.println("===>" + ne.getMessage());
            LOGGER.error("No files in home dir.", ne);
            exit(4);
        }

        System.out.println("===> 1");
        // get version of build
        String version = config.version();

        // get local resources pointers that will be set on the master container env
        String leaderJarPath = String.format("/bin/leader-%s-all.jar", version);
        LOGGER.info("Leader Jar path is: {}", leaderJarPath);
        Path mergedPath = Path.mergePaths(jarPath, new Path(leaderJarPath));

        // System.out.println("===> path: " + jarPathQualified);
        System.out.println("===> 2");
        LOGGER.info("Leader merged jar path is: {}", mergedPath);
        LocalResource leaderJar = null;
        LocalResource propFile = null;
        LocalResource log4jPropFile = null;

        try {
            System.out.println("===> 3");
            leaderJar = setLocalResourceFromPath(mergedPath);
            propFile = setLocalResourceFromPath(Path.mergePaths(jarPath, new Path("/amaterasu.properties")));
            log4jPropFile = setLocalResourceFromPath(Path.mergePaths(jarPath, new Path("/log4j.properties")));
            System.out.println("===> 4");
        } catch (IOException e) {
            LOGGER.error("Error initializing yarn local resources.", e);
            exit(4);
        }

        // set local resource on master container
        Map<String, LocalResource> localResources = new HashMap<>();
        localResources.put("leader.jar", leaderJar);
        localResources.put("amaterasu.properties", propFile);
        localResources.put("log4j.properties", log4jPropFile);
        amContainer.setLocalResources(localResources);

        // Setup CLASSPATH for ApplicationMaster
        Map<String, String> appMasterEnv = new HashMap<>();
        setupAppMasterEnv(appMasterEnv);
        appMasterEnv.put("AMA_CONF_PATH", String.format("%s/amaterasu.properties", config.YARN().hdfsJarsPath()));
        amContainer.setEnvironment(appMasterEnv);

        // Set up resource type requirements for ApplicationMaster
        Resource capability = Records.newRecord(Resource.class);
        capability.setMemory(config.YARN().master().memoryMB());
        capability.setVirtualCores(config.YARN().master().cores());

        // Finally, set-up ApplicationSubmissionContext for the application
        appContext.setApplicationName("amaterasu-" + opts.name);
        appContext.setAMContainerSpec(amContainer);
        appContext.setResource(capability);
        appContext.setQueue(config.YARN().queue());
        appContext.setPriority(Priority.newInstance(1));

        // Submit application
        ApplicationId appId = appContext.getApplicationId();
        LOGGER.info("Submitting application {}", appId);
        try {
            yarnClient.submitApplication(appContext);

        } catch (YarnException e) {
            LOGGER.error("Error submitting application.", e);
            exit(6);
        } catch (IOException e) {
            LOGGER.error("Error submitting application.", e);
            exit(7);
        }

        CuratorFramework client = CuratorFrameworkFactory.newClient(config.zk(),
                new ExponentialBackoffRetry(1000, 3));
        client.start();

        String newJobId = newId.replace("--new-job-id ", "");
        System.out.println("===> /" + newJobId + "-report-barrier");
        DistributedBarrier reportBarrier = new DistributedBarrier(client, "/" + newJobId + "-report-barrier");
        reportBarrier.setBarrier();
        reportBarrier.waitOnBarrier();

        String address = new String(client.getData().forPath("/" + newJobId + "/broker"));
        System.out.println("===> " + address);
        setupReportListener(address);

        ApplicationReport appReport = null;
        YarnApplicationState appState;

        do {
            try {
                appReport = yarnClient.getApplicationReport(appId);
            } catch (YarnException e) {
                LOGGER.error("Error getting application report.", e);
                exit(8);
            } catch (IOException e) {
                LOGGER.error("Error getting application report.", e);
                exit(9);
            }
            appState = appReport.getYarnApplicationState();
            if (isAppFinished(appState)) {
                exit(0);
                break;
            }
            //LOGGER.info("Application not finished ({})", appReport.getProgress());
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                LOGGER.error("Interrupted while waiting for job completion.", e);
                exit(137);
            }
        } while (!isAppFinished(appState));

        LOGGER.info("Application {} finished with state {}-{} at {}", appId, appState, appReport.getFinalApplicationStatus(), appReport.getFinishTime());
    }

    private boolean isAppFinished(YarnApplicationState appState) {
        return appState == YarnApplicationState.FINISHED ||
                appState == YarnApplicationState.KILLED ||
                appState == YarnApplicationState.FAILED;
    }

    public static void main(String[] args) throws Exception {
        Client c = new Client();

        JobOpts opts = ArgsParser.getJobOpts(args);

        c.run(opts, args);
    }

    private static String joinStrings(String[] str) {

        StringBuilder builder = new StringBuilder();
        for (String s : str) {
            builder.append(s);
            builder.append(" ");
        }
        return builder.toString();

    }

    private void setupReportListener(String address) throws JMSException {

        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory(address);
        Connection conn = cf.createConnection();
        conn.start();

        Session session = conn.createSession(false, Session.AUTO_ACKNOWLEDGE);

        //TODO: move to a const in common
        Topic destination = session.createTopic("JOB.REPORT");

        MessageConsumer consumer = session.createConsumer(destination);
        consumer.setMessageListener(new ActiveReportListener());

    }

    private void setupAppMasterEnv(Map<String, String> appMasterEnv) {
        Apps.addToEnvironment(appMasterEnv,
                ApplicationConstants.Environment.CLASSPATH.name(),
                ApplicationConstants.Environment.PWD.$() + File.separator + "*", File.pathSeparator);

        for (String c : conf.getStrings(
                YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                YarnConfiguration.DEFAULT_YARN_APPLICATION_CLASSPATH)) {
            Apps.addToEnvironment(appMasterEnv, ApplicationConstants.Environment.CLASSPATH.name(),
                    c.trim(), File.pathSeparator);
        }
    }
}