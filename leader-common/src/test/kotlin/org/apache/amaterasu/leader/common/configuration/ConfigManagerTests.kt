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
import org.jetbrains.spek.api.dsl.given
import org.jetbrains.spek.api.dsl.it
import org.jetbrains.spek.api.dsl.on
import java.io.File
import kotlin.test.assertEquals

class ConfigManagerTests : Spek({

    val marker = this.javaClass.getResource("/maki.yml").path

    given("a ConfigManager for a job ") {

        val repoPath = "${File(marker).parent}/test_repo"
        val cfg = ConfigManager("test", repoPath)

        it("loads the job level environment"){
            assertEquals(cfg.config[Job.master] , "yarn")
        }

        on("getting an env for an action with default path") {
            val startConf = cfg.getActionConfiguration("start")
            it("loads the specific configuration defined in the actions folder"){
                assertEquals(startConf[Job.master] , "mesos")
            }
        }

        on("getting an env for an action with a conf: property in the maki.yml"){
            val step2conf = cfg.getActionConfiguration("step2", "src/{action_name}/{env}/")

            it("loads the specific configuration defined in the actions folder"){
                assertEquals(step2conf[Job.name] , "test2")
            }
        }

        on("getting an env for an action with no action level config"){
            val step3conf = cfg.getActionConfiguration("step3")

            it("loads only the job level conf"){
                assertEquals(step3conf[Job.name] , "test")
            }
        }

        on("receiving a path to a specific file" ){
            val step4conf = cfg.getActionConfiguration("step4", "src/start/env/{env}/job.yml")

            it("loads the specific configuration from the file"){
                assertEquals(step4conf[Job.master] , "mesos")
            }
        }


    }

    given("a ConfigManager for a job with spark framework") {

        val repoPath = "${File(marker).parent}/spark_repo"
        val cfg = ConfigManager("test", repoPath, listOf("sparkConfiguration"))

        it("load the framework configuration for spark"){
            val spark: Map<String, String> = cfg.config["sparkConfiguration"]
            assertEquals(spark["spark.executor.memory"], "1g")
        }
    }
})