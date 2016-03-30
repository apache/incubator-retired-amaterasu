package io.shinto.amaterasu.mesos

import io.shinto.amaterasu.Logging
import io.shinto.amaterasu.configuration.ClusterConfig
import io.shinto.amaterasu.mesos.schedulers.JobScheduler
import io.shinto.amaterasu.utilities.FsUtil
import org.apache.mesos.Protos.FrameworkID

import org.apache.mesos.{ MesosSchedulerDriver, Protos }

case class Args(
  repo: String = "",
  branch: String = "master",
  name: String = "amaterasu-job",
  jobId: String = null
)

/**
  * The JobLauncher allows the execution of a single job, without creating a full
  * Amaterasu cluster (no cluster scheduler).
  */
object JobLauncher extends App with Logging {

  val parser = new scopt.OptionParser[Args]("amaterasu job") {
    head("amaterasu job", "0.1.0") //TODO: Get the version from the build

    opt[String]('r', "repo") action { (x, c) =>
      c.copy(repo = x)
    } text "The git repo containing the job"
    opt[String]('b', "branch") action { (x, c) =>
      c.copy(branch = x)
    } text "The branch to be executed (default is master)"
    opt[String]('n', "name") action { (x, c) =>
      c.copy(name = x)
    } text "The name of the job"
    opt[String]('i', "job-id") action { (x, c) =>
      c.copy(jobId = x)
    } text "The jobId - should be passed only when resuming a job"

  }

  parser.parse(args, Args()) match {
    case Some(arguments) =>

      val config = ClusterConfig()
      FsUtil(config).distributeJar()

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

      log.debug(s"The framework user is ${config.user}")
      val masterAddress = s"${config.master}:${config.masterPort}"
      val scheduler = JobScheduler(arguments.repo, arguments.branch, resume, config)
      val driver = new MesosSchedulerDriver(scheduler, framework, masterAddress)

      log.debug(s"Connecting to master on: $masterAddress")
      driver.run()

    case None =>
    // arguments are bad, error message will have been displayed
  }

}
