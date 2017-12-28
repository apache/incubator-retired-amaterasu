package org.apache.amaterasu.leader.utilities

import java.io.FileInputStream

import org.apache.amaterasu.common.configuration.ClusterConfig
import org.apache.amaterasu.common.logging.Logging


abstract class BaseJobLauncher extends App with Logging {

  def run(args: Args, config: ClusterConfig, resume: Boolean): Unit = ???

  val parser = Args.getParser
  parser.parse(args, Args()) match {

    case Some(arguments: Args) =>

      val config = ClusterConfig(new FileInputStream(s"${arguments.home}/amaterasu.properties"))
      val resume = arguments.jobId != null

      run(arguments, config, resume)

    case None =>
    // arguments are bad, error message will have been displayed
  }
}
