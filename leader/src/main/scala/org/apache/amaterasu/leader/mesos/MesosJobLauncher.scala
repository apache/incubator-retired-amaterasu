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

import org.apache.amaterasu.common.configuration.ClusterConfig
import org.apache.amaterasu.leader.mesos.schedulers.JobScheduler
import org.apache.amaterasu.leader.utilities.{Args, BaseJobLauncher}
import org.apache.log4j.LogManager
import org.apache.mesos.Protos.FrameworkID
import org.apache.mesos.{MesosSchedulerDriver, Protos}

/**
  * The JobLauncher allows the execution of a single job, without creating a full
  * Amaterasu cluster (no cluster scheduler).
  */
object MesosJobLauncher extends BaseJobLauncher {

  override def run(arguments: Args, config: ClusterConfig, resume: Boolean): Unit = {
    LogManager.resetConfiguration()
    val frameworkBuilder = Protos.FrameworkInfo.newBuilder()
      .setName(s"${arguments.name} - Amaterasu Job")
      .setFailoverTimeout(config.timeout)
      .setUser(config.user)

    // TODO: test this
    if (resume) {
      frameworkBuilder.setId(FrameworkID.newBuilder().setValue(arguments.jobId))
    }

    val framework = frameworkBuilder.build()

    val masterAddress = s"${config.master}:${config.masterPort}"

    val scheduler = JobScheduler(
      arguments.repo,
      arguments.branch,
      arguments.username,
      arguments.password,
      arguments.env,
      resume,
      config,
      arguments.report,
      arguments.home
    )

    val driver = new MesosSchedulerDriver(scheduler, framework, masterAddress)

    log.debug(s"Connecting to master on: $masterAddress")
    driver.run()
  }
}
