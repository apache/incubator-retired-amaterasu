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

import java.util
import java.util.UUID

import org.apache.mesos.Protos._


object MesosTestUtil {
  def createOffer(mem: Double, disk: Double, cpus: Int): Offer = {

    val resources = new util.ArrayList[Resource]()
    resources.add(createScalarResource("mem", mem))
    resources.add(createScalarResource("disk", disk))
    resources.add(createScalarResource("cpus", cpus))

    Offer.newBuilder()
      .setId(OfferID.newBuilder().setValue(UUID.randomUUID.toString))
      .setFrameworkId(FrameworkID.newBuilder().setValue("Amaterasu"))
      .setSlaveId(SlaveID.newBuilder().setValue(UUID.randomUUID.toString))
      .setHostname("localhost")
      .addAllResources(resources)

      .build()

  }

  def createScalarResource(name: String, value: Double): org.apache.mesos.Protos.Resource = {

    org.apache.mesos.Protos.Resource.newBuilder().setName(name).setType(Value.Type.SCALAR)
      .setScalar(Value.Scalar.newBuilder.setValue(value).build()).build()

  }
}
