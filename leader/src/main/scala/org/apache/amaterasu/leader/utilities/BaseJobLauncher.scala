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
package org.apache.amaterasu.leader.utilities

import java.io.FileInputStream

import org.apache.amaterasu.common.configuration.ClusterConfig
import org.apache.amaterasu.common.logging.Logging


abstract class BaseJobLauncher extends App with Logging {

  def run(args: Args, config: ClusterConfig, resume: Boolean): Unit = ???

  val parser = Args.getParser
  parser.parse(args, Args()) match {

    case Some(arguments: Args) =>

      val config = ClusterConfig(new FileInputStream(s"${arguments.configHome}/amaterasu.properties"))
      val resume = arguments.jobId != null

      run(arguments, config, resume)

    case None =>
    // arguments are bad, error message will have been displayed
  }
}
