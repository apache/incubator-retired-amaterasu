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

data class AmaOpts(
        var repo: String = "",
        var branch: String = "master",
        var env: String = "default",
        var name: String = "amaterasu-job",
        var jobId: String = "",
        var newJobId: String = "",
        var report: String = "code",
        var home: String = "") {

    fun toCmdString(): String {

        var cmd = " --repo $repo --branch $branch --env $env --name $name --report $report --home $home"
        if (jobId.isNotEmpty()) {
            cmd += " --job-id $jobId"
        }
        return cmd
    }

    override fun toString(): String {
        return toCmdString()
    }
}