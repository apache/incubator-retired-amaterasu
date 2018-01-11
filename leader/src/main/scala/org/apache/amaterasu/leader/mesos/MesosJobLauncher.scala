package org.apache.amaterasu.leader.mesos

import org.apache.amaterasu.common.configuration.ClusterConfig
import org.apache.amaterasu.leader.mesos.schedulers.JobScheduler
import org.apache.amaterasu.leader.utilities.{Args, BaseJobLauncher}
import org.apache.mesos.Protos.FrameworkID
import org.apache.mesos.{MesosSchedulerDriver, Protos}

/**
  * The JobLauncher allows the execution of a single job, without creating a full
  * Amaterasu cluster (no cluster scheduler).
  */
object MesosJobLauncher extends BaseJobLauncher {

  override def run(arguments: Args, config: ClusterConfig, resume: Boolean): Unit = {
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
