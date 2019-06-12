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
package org.apache.amaterasu.leader.common.utilities

import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.given
import org.jetbrains.spek.api.dsl.it
import org.jsoup.Jsoup
import kotlin.test.assertEquals

class HttpServerTests : Spek({
    val resources = this::class.java.getResource("/jetty-test-data.txt").path.removeSuffix("/jetty-test-data.txt")

    given("an Amaterasu Web server is started") {
        val server = HttpServer()
        server.start("8002", resources)

        it("serve content and stop successfully") {
            lateinit var data: String
            try {
                val html = Jsoup.connect("http://localhost:8002/jetty-test-data.txt").get().select("body").text()
                data = html.toString()
            } finally {
                server.stop()
            }
            assertEquals(data, "This is a test file to download from Jetty webserver")
        }
    }

    given("an Amaterasu Web server is started pointing to a sub dir with two files") {
        val server = HttpServer()
        server.start("8001", "$resources/dist")

        it("list the files correctly") {
            try {
                val urls = server.getFilesInDirectory("localhost", "8001", "")
                assertEquals(urls.size, 2)
            } finally {
                server.stop()
            }
        }
    }
})