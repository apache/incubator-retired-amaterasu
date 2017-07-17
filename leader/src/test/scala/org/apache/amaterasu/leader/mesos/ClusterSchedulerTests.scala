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
import org.apache.amaterasu.leader.mesos.schedulers.ClusterScheduler
import org.apache.amaterasu.leader.Kami
import org.scalatest._

class ClusterSchedulerTests extends FlatSpec with Matchers {

  "an offer" should "be accepted if has enough resources" in {

    val kami = Kami()
    val config = ClusterConfig(getClass.getResourceAsStream("/amaterasu.properties"))
    config.Jobs.cpus = 1
    config.Jobs.mem = 1024
    config.Jobs.repoSize = 1024

    val scheduler = ClusterScheduler(kami, config)
    val offer = MesosTestUtil.createOffer(2000, 2000, 2)
    val res = scheduler.validateOffer(offer)

    res should be(true)

  }

  it should "not be accepted if has missing resources" in {

    val kami = Kami()
    val config = ClusterConfig(getClass.getResourceAsStream("/amaterasu.properties"))
    config.Jobs.cpus = 1
    config.Jobs.mem = 1024
    config.Jobs.repoSize = 1024

    val scheduler = ClusterScheduler(kami, config)
    val offer = MesosTestUtil.createOffer(2000, 128, 2)
    val res = scheduler.validateOffer(offer)

    res should be(false)

  }

}