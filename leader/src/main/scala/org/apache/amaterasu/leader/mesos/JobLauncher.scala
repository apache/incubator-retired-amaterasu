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

import java.io.FileInputStream
import java.nio.file.Paths

import org.apache.amaterasu.common.logging.Logging
import org.apache.amaterasu.common.configuration.ClusterConfig
import org.apache.amaterasu.leader.mesos.schedulers.JobScheduler
import org.apache.mesos.Protos.FrameworkID
import org.apache.mesos.{MesosSchedulerDriver, Protos}

case class Args(
                 repo: String = "",
                 branch: String = "master",
                 env: String = "default",
                 name: String = "amaterasu-job",
                 jobId: String = null,
                 report: String = "code",
                 home: String = ""
               )

/**
  * The JobLauncher allows the execution of a single job, without creating a full
  * Amaterasu cluster (no cluster scheduler).
  */
object JobLauncher extends App with Logging {


  val parser = new scopt.OptionParser[Args]("amaterasu job") {
    head("amaterasu job", "0.2.0-incubating") //TODO: Get the version from the build

    opt[String]('r', "repo") action { (x, c) =>
      c.copy(repo = x)
    } text "The git repo containing the job"
    opt[String]('b', "branch") action { (x, c) =>
      c.copy(branch = x)
    } text "The branch to be executed (default is master)"
    opt[String]('e', "env") action { (x, c) =>
      c.copy(env = x)
    } text "The environment to be executed (test, prod, etc. values from the default env are taken if np env specified)"
    opt[String]('n', "name") action { (x, c) =>
      c.copy(name = x)
    } text "The name of the job"
    opt[String]('i', "job-id") action { (x, c) =>
      c.copy(jobId = x)
    } text "The jobId - should be passed only when resuming a job"
    opt[String]('r', "report") action { (x, c) =>
      c.copy(report = x)
    }
    opt[String]('h', "home") action { (x, c) =>
      c.copy(home = x)
    } text "The level of reporting"

  }


  parser.parse(args, Args()) match {

    case Some(arguments) =>

      val config = ClusterConfig(new FileInputStream(s"${arguments.home}/amaterasu.properties"))

      val frameworkBuilder = Protos.FrameworkInfo.newBuilder()
        .setName(s"${arguments.name} - Amaterasu Job")
        .setFailoverTimeout(config.timeout)
        .setUser(config.user)

      // TODO: test this
      val resume = arguments.jobId != null
      if (resume) {
        frameworkBuilder.setId(FrameworkID.newBuilder().setValue(arguments.jobId))
      }

      val framework = frameworkBuilder.build()

      val masterAddress = s"${config.master}:${config.masterPort}"

      val scheduler = JobScheduler(
        arguments.repo,
        arguments.branch,
        arguments.env,
        resume,
        config,
        arguments.report,
        arguments.home
      )

      val driver = new MesosSchedulerDriver(scheduler, framework, masterAddress)

      log.debug(s"Connecting to master on: $masterAddress")
      driver.run()

    case None =>
    // arguments are bad, error message will have been displayed
  }

}
