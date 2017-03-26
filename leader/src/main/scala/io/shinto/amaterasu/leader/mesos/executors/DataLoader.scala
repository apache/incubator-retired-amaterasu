package io.shinto.amaterasu.leader.mesos.executors

import java.nio.file.{ Files, Paths }

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule

import io.shinto.amaterasu.common.logging.Logging
import io.shinto.amaterasu.common.execution.dependencies.Dependencies
import io.shinto.amaterasu.common.runtime.Environment

import org.apache.mesos.protobuf.ByteString

import io.shinto.amaterasu.common.dataobjects._

import scala.io.Source

/**
  * Created by karel_alfonso on 27/06/2016.
  */
object DataLoader extends Logging {

  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)

  def getTaskData(actionData: ActionData, env: String): ByteString = {

    val srcFile = actionData.src
    val src = Source.fromFile(s"repo/src/${srcFile}").mkString
    val envValue = Source.fromFile(s"repo/env/${env}.json").mkString

    val envData = mapper.readValue(envValue, classOf[Environment])

    val data = mapper.writeValueAsBytes(new TaskData(src, envData))
    ByteString.copyFrom(data)

  }

  def getExecutorData(env: String): ByteString = {

    val ymlMapper = new ObjectMapper(new YAMLFactory())
    ymlMapper.registerModule(DefaultScalaModule)

    val envValue = Source.fromFile(s"repo/env/${env}.json").mkString
    val envData = mapper.readValue(envValue, classOf[Environment])

    var depsData: Dependencies = null

    if (Files.exists(Paths.get("repo/deps/jars.yml"))) {
      val depsValue = Source.fromFile(s"repo/deps/jars.yml").mkString
      depsData = ymlMapper.readValue(depsValue, classOf[Dependencies])
    }

    val data = mapper.writeValueAsBytes(new ExecData(envData, depsData))
    ByteString.copyFrom(data)
  }

}