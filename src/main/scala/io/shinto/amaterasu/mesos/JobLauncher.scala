package io.shinto.amaterasu.mesos

import io.shinto.amaterasu.{Config, Logging}
import io.shinto.amaterasu.utilities.FsUtil

/**
  * The JobLauncher allows the execution of a single job, without creating a full
  * Amaterasu cluster (no cluster scheduler).
  */
object JobLauncher extends App with Logging{

  val config = Config()
  FsUtil(config).distributeJar()

}
