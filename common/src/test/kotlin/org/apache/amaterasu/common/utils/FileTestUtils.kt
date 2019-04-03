package org.apache.amaterasu.common.utils

import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.given
import org.jetbrains.spek.api.dsl.it
import org.jetbrains.spek.api.dsl.on

import org.junit.Assert.*

class FileUtilTest: Spek({

    given("an s3 url") {
        //val url = "https://s3-ap-southeast-2.amazonaws.com/amaterasu/BugBounty-TestUpload.txt"
        val url = "https://s3-ap-southeast-2.amazonaws.com/amaterasu/testfile.txt"
        on("downloading file from s3") {
            val result: Boolean = FileUtil.downloadFile(url)
            it("is successful") {
                assertEquals(true, result)
            }
        }
    }

})