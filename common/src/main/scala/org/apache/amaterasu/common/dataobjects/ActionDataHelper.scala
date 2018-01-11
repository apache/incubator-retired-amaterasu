package org.apache.amaterasu.common.dataobjects

import com.google.gson.Gson

object ActionDataHelper {
  private val gson = new Gson
  def toJsonString(actionData: ActionData): String = {
    gson.toJson(actionData)
  }

  def fromJsonString(jsonString: String) : ActionData = {
    gson.fromJson[ActionData](jsonString, ActionData.getClass)
  }
}