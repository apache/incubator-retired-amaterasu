package org.apache.amaterasu.leader.common.dsl

import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.given
import org.jetbrains.spek.api.dsl.it
import java.io.File

class GitUtilTests : Spek({
    given("a repo and a branch") {
        val repo = "https://github.com/shintoio/amaterasu-job-sample.git"
        val branch = "feature/new-format"

        it("clones the repo successfully") {
            GitUtil.cloneRepo(repo, branch)
            assert(File("repo").exists())
        }
    }
})