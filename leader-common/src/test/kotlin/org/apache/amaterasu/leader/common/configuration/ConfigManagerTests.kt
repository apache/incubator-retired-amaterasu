package org.apache.amaterasu.leader.common.configuration

import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.describe
import org.jetbrains.spek.api.dsl.it
import org.jetbrains.spek.api.dsl.xcontext
import java.io.File

class ConfigManagerTests : Spek({

    describe("creating a ConfigManager for a job with ") {

        val marker = javaClass.getResource("marker.yml").path
        val repoPath = "${File(marker).parent}/test_repo"
        val cfg = ConfigManager("test", repoPath)

        it("loads the job level environment"){
            assert(cfg.config[Job.master] == "yarn")
        }

        xcontext("when an action is loaded") {
            it("loads the specific configuration for the action"){
                assert(cfg.config[Job.master] == "mesos")
            }
        }
    }
})