package io.shinto.amaterasu.mesos.executors

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import io.shinto.amaterasu.Logging
import io.shinto.amaterasu.runtime.Environment
import org.apache.mesos.protobuf.ByteString
//import com.google.protobuf.ByteString
import io.shinto.amaterasu.dataObjects.ActionData

import scala.io.Source

/**
  * Created by karel_alfonso on 27/06/2016.
  */
object DataLoader extends Logging {

  def getTaskData(actionData: ActionData, env: String): ByteString = {

    val srcFile = actionData.src
    val src = Source.fromFile(s"repo/src/${srcFile}").mkString
    val envValue = Source.fromFile(s"repo/env/${env}.json").mkString

    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)

    val envData = mapper.readValue(envValue, classOf[Environment])

    val data = mapper.writeValueAsBytes(new TaskData(src, envData))
    ByteString.copyFrom(data)

  }

  def getExecutorData(env: String): ByteString = {

    val data = Source.fromFile(s"repo/env/$env.json").mkString.getBytes()
    ByteString.copyFrom(data)
  }

}

case class TaskData(src: String, env: Environment)
