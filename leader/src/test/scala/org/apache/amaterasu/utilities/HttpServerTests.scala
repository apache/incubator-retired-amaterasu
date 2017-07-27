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
package org.apache.amaterasu.utilities

import java.io.File

import org.apache.amaterasu.leader.utilities.HttpServer
import org.scalatest.{FlatSpec, Matchers}

import scala.io.Source

/**
  * Created by kirupa on 16/10/16.
  */
class HttpServerTests extends FlatSpec with Matchers {

  // this is an ugly hack, getClass.getResource("/").getPath should have worked but
  // stopped working when we moved to gradle :(
  private val resources = new File(getClass.getResource("/simple-maki.yml").getPath).getParent

  "Jetty Web server" should "start HTTP server, serve content and stop successfully" in {
    var data = ""
    try {
      HttpServer.start("8000", resources)
      val html = Source.fromURL("http://localhost:8000/jetty-test-data.txt")
      data = html.mkString
    }
    finally {
      HttpServer.stop()
    }
    data should equal("This is a test file to download from Jetty webserver")
  }
}
