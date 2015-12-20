package io.shinto.amaterasu.dsl

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import io.shinto.amaterasu.execution.JobManager
import scala.io.Source

/**
  * The JobParser class is in charge of parsing the maki.yaml file which
  * describes the workflow of an amaterasu job
  */
object JobParser {

  def loadMakiFile(): String = {

    Source.fromFile("repo/maki.yaml").mkString

  }

  def parse(maki: String): JobManager = {

    val mapper = new ObjectMapper(new YAMLFactory())
    mapper.readValue(maki, classOf[JobManager])
  }

}