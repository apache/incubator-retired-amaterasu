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

import org.apache.amaterasu.common.configuration.ClusterConfig
import org.apache.amaterasu.common.logging.Logging
import org.apache.amaterasu.leader.Kami
import org.apache.amaterasu.leader.mesos.schedulers.ClusterScheduler
import org.apache.mesos.{MesosSchedulerDriver, Protos}

object Launcher extends App with Logging {

  println(
    """
       Apache
           (                      )
           )\        )      )   ( /(   (   (       )        (
          ((_)(     (     ( /(  )\()  ))\  )(   ( /(  (    ))\
         )\ _ )\    )\  ' )(_))(_))/ /((_)(()\  )(_)) )\  /((_)
         (_)_\(_) _((_)) ((_) _ | |_ (_))   ((_)((_)_ ((_)(_))(
          / _ \  | '   \()/ _` ||  _|/ -_) | '_|/ _` |(_-<| || |
         /_/ \_\ |_|_|_|  \__,_| \__|\___| |_|  \__,_|/__/ \_,_|

         Durable Dataflow Cluster
         Version 0.1.0
    """
  )

  val config = ClusterConfig(new FileInputStream("scripts/amaterasu.properties"))
  val kami = Kami(Seq("https://github.com/roadan/amaterasu-job-sample.git"))

  // for multi-tenancy reasons the name of the framework is composed out of the username ( which defaults
  // to empty string concatenated with - Amaterasu
  val framework = Protos.FrameworkInfo.newBuilder()
    .setName(s"${config.user} - Amaterasu")
    .setFailoverTimeout(config.timeout)
    .setUser(config.user).build()

  log.debug(s"The framework user is ${config.user}")
  val masterAddress = s"${config.master}:${config.masterPort}"
  val scheduler = ClusterScheduler(kami, config)
  val driver = new MesosSchedulerDriver(scheduler, framework, masterAddress)

  log.debug(s"Connecting to master on: $masterAddress")
  driver.run()

}