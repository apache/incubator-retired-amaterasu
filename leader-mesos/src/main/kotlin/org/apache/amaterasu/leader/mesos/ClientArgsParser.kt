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

import org.apache.amaterasu.leader.common.configuration.ClusterConfig
import org.apache.amaterasu.leader.common.launcher.AmaOpts
import org.apache.amaterasu.leader.common.launcher.ArgsParser
import org.apache.log4j.LogManager
import org.apache.mesos.MesosSchedulerDriver
import org.apache.mesos.Protos
import java.io.FileInputStream

class ClientArgsParser : ArgsParser() {

    override fun run() {

        val opts = AmaOpts(repo, branch, env, name, jobId, newJobId, report, home)

        val config = ClusterConfig.apply(FileInputStream("${opts.home}/amaterasu.properties"))
        val resume = opts.jobId.isNotEmpty()

        LogManager.resetConfiguration()

        val frameworkBuilder = Protos.FrameworkInfo.newBuilder()
                .setName("${opts.name} - Amaterasu Job")
                .setFailoverTimeout(config.timeout())
                .setUser(config.user())

        if (resume) {
            frameworkBuilder.setId(Protos.FrameworkID.newBuilder().setValue(opts.jobId))
        }

        val framework = frameworkBuilder.build()

        val masterAddress = "${config.master()}:${config.masterPort()}"

        val scheduler = JobScheduler(
                opts.repo,
                opts.branch,
                opts.userName,
                opts.password,
                opts.env,
                resume,
                config,
                opts.report,
                opts.home
        )

        val driver = MesosSchedulerDriver(scheduler, framework, masterAddress)

        driver.run()

    }
}

