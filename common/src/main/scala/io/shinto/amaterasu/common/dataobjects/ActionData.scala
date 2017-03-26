package io.shinto.amaterasu.common.dataobjects

import io.shinto.amaterasu.enums.ActionStatus.ActionStatus

import scala.collection.mutable.ListBuffer

case class ActionData(var status: ActionStatus,
                      name: String,
                      src: String,
                      actionType: String,
                      id: String,
                      nextActionIds: ListBuffer[String]) {
  var errorActionId: String = null
}