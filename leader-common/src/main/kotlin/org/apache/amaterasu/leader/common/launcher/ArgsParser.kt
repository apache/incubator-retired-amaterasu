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
package org.apache.amaterasu.leader.common.launcher

import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.options.prompt

abstract class ArgsParser : CliktCommand() {

     val repo: String by option("--repo", help = "The service address").prompt("Please provide an Amaterasu Reop")
     val branch: String by option(help = "The branch to be executed (default is master)").default("master")
     val env: String by option(help = "The environment to be executed (test, prod, etc. values from the default env are taken if np env specified)").default("default")
     val name: String by option(help = "The name of the job").default("amaterasu-job")
     val jobId: String by option("--job-id", help = "The jobId - should be passed only when resuming a job").default("")
     val newJobId: String by option("--new-job-id", help = "The jobId - should never be passed by a user").default("")
     val report: String by option(help = "The level of reporting").default("code")
     val home: String by option(help = "").default("")

}