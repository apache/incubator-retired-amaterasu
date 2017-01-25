package io.shinto.amaterasu.leader.mesos

import java.io.FileInputStream

import io.shinto.amaterasu.common.configuration.ClusterConfig
import io.shinto.amaterasu.leader.mesos.schedulers.ClusterScheduler
import io.shinto.amaterasu.Kami
import io.shinto.amaterasu.common.logging.Logging

import org.apache.mesos.{ Protos, MesosSchedulerDriver }

object Launcher extends App with Logging {

  println(
    """
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

  val config = ClusterConfig(new FileInputStream("./amaterasu.properties"))
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