package io.shinto.amaterasu.leader.mesos

import java.util
import java.util.UUID

import org.apache.mesos.Protos._

/**
  * Created by roadan on 10/17/15.
  */
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
