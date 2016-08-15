package io.shinto.amaterasu.mesos.executors

import org.apache.mesos.protobuf.ByteString
import io.shinto.amaterasu.dataObjects.ActionData

import scala.io.Source

/**
  * Created by karel_alfonso on 27/06/2016.
  */
class TaskData(actionData: ActionData) {
  def toTaskData(): ByteString = {
    val srcFile = actionData.src
    val src = Source.fromFile(s"repo/src/${srcFile}").mkString
    ByteString.copyFrom(src.getBytes("UTF-8"))
  }

}
