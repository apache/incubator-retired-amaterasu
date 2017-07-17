/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.amaterasu.leader.execution.actions

import org.apache.amaterasu.common.dataobjects.ActionData
import org.apache.amaterasu.common.logging.Logging
import org.apache.amaterasu.enums.ActionStatus
import org.apache.curator.framework.CuratorFramework

trait Action extends Logging {

  // this is the znode path for the action
  var actionPath: String = _
  var actionId: String = _

  var data: ActionData = null
  var client: CuratorFramework = null

  def execute(): Unit

  def handleFailure(message: String): String

  /**
    * The announceStart register the beginning of the of the task with ZooKeper
    */
  def announceStart: Unit = {

    log.debug(s"Starting action ${data.name} of group ${data.groupId} and type ${data.typeId}")
    client.setData().forPath(actionPath, ActionStatus.started.toString.getBytes)
    data.status = ActionStatus.started
  }

  def announceQueued: Unit = {

    log.debug(s"Action ${data.name} of group ${data.groupId} and of type ${data.typeId} is queued for execution")
    client.setData().forPath(actionPath, ActionStatus.queued.toString.getBytes)
    data.status = ActionStatus.queued
  }

  def announceComplete: Unit = {

    log.debug(s"Action ${data.name} of group ${data.groupId} and of type ${data.typeId} completed")
    client.setData().forPath(actionPath, ActionStatus.complete.toString.getBytes)
    data.status = ActionStatus.complete
  }

  def announceCanceled: Unit = {

    log.debug(s"Action ${data.name} of group ${data.groupId} and of type ${data.typeId} was canceled")
    client.setData().forPath(actionPath, ActionStatus.canceled.toString.getBytes)
    data.status = ActionStatus.canceled
  }
  protected def announceFailure(): Unit = {}

}