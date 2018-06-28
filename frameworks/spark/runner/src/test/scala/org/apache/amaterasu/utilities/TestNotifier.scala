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
package org.apache.amaterasu.utilities

import org.apache.amaterasu.common.execution.actions.Notifier
import org.apache.amaterasu.common.logging.Logging


class TestNotifier extends Notifier with Logging {

  override def info(msg: String): Unit = {
    log.info(msg)
  }

  override def success(line: String): Unit = {
    log.info(s"successfully executed line: $line")
  }

  override def error(line: String, msg: String): Unit = {
    log.error(s"Error executing line: $line message: $msg")
  }
}
