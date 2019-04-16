//package org.apache.amaterasu.common.utils
//
//import org.jetbrains.spek.api.Spek
//import org.jetbrains.spek.api.dsl.given
//import org.jetbrains.spek.api.dsl.it
//import org.jetbrains.spek.api.dsl.on
//
//import org.junit.Assert.*
//import java.io.File
//
//class FileUtilTest: Spek({
//
//    given("an s3 url") {
//        val url = "https://s3-ap-southeast-2.amazonaws.com/amaterasu/testfile.txt"
//        val util =  FileUtil("", "")
//        on("downloading file from s3") {
//            val result: Boolean = util.downloadFile(url,"testfile.txt")
//            it("is successful") {
//                val resultFile = File("testfile.txt")
//                assert(resultFile.exists())
//            }
//        }
//    }
//
//})