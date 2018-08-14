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
package org.apache.amaterasu.leader.common.configuration

import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.*
import java.io.File

class ConfigManagerTests : Spek({

    describe("creating a ConfigManager for a job with ") {

        val marker = this.javaClass.getResource("/maki.yml").path
        val repoPath = "${File(marker).parent}/test_repo"
        val cfg = ConfigManager("test", repoPath)

        it("loads the job level environment"){
            assert(cfg.config[Job.master] == "yarn")
        }

        on("getting an env for an action with default path") {
            val startConf = cfg.getActionConfiguration("start")
            it("loads the specific configuration defined in the actions folder"){
                assert(startConf[Job.master] == "mesos")
            }
        }

        on("getting an env for an action with a conf: property in the maki.yml"){
            val step2conf = cfg.getActionConfiguration("step2", "src/{action_name}/{env}/")

            it("loads the specific configuration defined in the actions folder"){
                assert(step2conf[Job.name] == "step2")
            }
        }
    }
})