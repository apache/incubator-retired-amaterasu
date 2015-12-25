package io.shinto.amaterasu.dataObjects

import scala.collection.mutable.ListBuffer

case class ActionData(
  name: String,
  src: String,
  actionType: String,
  id: String,
  nextActionIds: ListBuffer[String],
  errorActionId: String
)