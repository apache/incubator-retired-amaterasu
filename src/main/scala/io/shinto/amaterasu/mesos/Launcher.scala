package io.shinto.amaterasu.mesos

import org.apache.mesos.{ Protos, MesosSchedulerDriver }
import io.shinto.amaterasu.{ Kami, Config }

object Launcher {

  def main(args: Array[String]) {

    println(
      """
           (                      )
           )\        )      )   ( /(   (   (       )        (
          ((_)(     (     ( /(  )\()  ))\  )(   ( /(  (    ))\
         )\ _ )\    )\  ' )(_))(_))/ /((_)(()\  )(_)) )\  /((_)
         (_)_\(_) _((_)) ((_) _ | |_ (_))   ((_)((_)_ ((_)(_))(
          / _ \  | '   \()/ _` ||  _|/ -_) | '_|/ _` |(_-<| || |
         /_/ \_\ |_|_|_|  \__,_| \__|\___| |_|  \__,_|/__/ \_,_|

         Durable Workflow Cluster
         Version 0.1.0
      """
    )

    val config = Config()
    val kami = Kami()

    // for multi-tenancy reasons the name of the framework is composed out of the username ( which defaults
    // to empty string concatenated with - Amaterasu
    val framework = Protos.FrameworkInfo.newBuilder()
      .setName(s"${config.user} - Amaterasu")
      .setFailoverTimeout(config.timeout)
      .setUser(config.user).build()

    val scheduler = ClusterScheduler(kami, config)
    val driver = new MesosSchedulerDriver(scheduler, framework, s"${config.master}:5050")
    driver.run()

  }

}
