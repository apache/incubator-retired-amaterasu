package io.shinto.amaterasu.mesos.executors

import java.io.ByteArrayInputStream

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import io.shinto.amaterasu.Logging
import org.apache.mesos.protobuf.ByteString

import io.shinto.amaterasu.configuration.environments.Environment
import io.shinto.amaterasu.dataObjects.ActionData

import scala.io.Source

/**
  * Created by karel_alfonso on 27/06/2016.
  */
class TaskDataLoader(actionData: ActionData, env: String) extends Logging {

  def toTaskData(): ByteString = {

    val srcFile = actionData.src
    val src = Source.fromFile(s"repo/src/${srcFile}").mkString
    val envValue = Source.fromFile(s"repo/env/${env}.json").mkString

    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)

    val envData = mapper.readValue(envValue, classOf[Environment])

    //val data = s"""{"src":"$src", "env":$envValue}"""
    val data = mapper.writeValueAsBytes(new TaskData(src, envData))
    ByteString.copyFrom(data)

  }

}

case class TaskData(src: String, env: Environment)
