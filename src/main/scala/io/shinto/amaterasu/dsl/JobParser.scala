package io.shinto.amaterasu.dsl

import io.shinto.amaterasu.execution.JobManager
import org.yaml.snakeyaml.Yaml

/**
  * The JobParser class is in charge of parsing the maki.yaml file which
  * describes the workflow of an amaterasu job
  */
class JobParser {

  def parse(maki: String): JobManager = {

    val yaml = new Yaml()
    yaml.load(maki)

    null
  }

}