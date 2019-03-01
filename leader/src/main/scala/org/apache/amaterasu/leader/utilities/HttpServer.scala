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
package org.apache.amaterasu.leader.utilities

import org.apache.amaterasu.common.logging.Logging
import org.apache.log4j.{BasicConfigurator, Level, Logger}
import org.eclipse.jetty.server.handler._
import org.eclipse.jetty.server.{Server, ServerConnector}
import org.eclipse.jetty.util.log.StdErrLog
import org.jsoup.Jsoup
import org.jsoup.select.Elements

import scala.collection.JavaConverters._
import scala.io.{BufferedSource, Source}

/**
  * Implementation of Jetty Web server to server Amaterasu libraries and other distribution files
  */
object HttpServer extends Logging {

  var server: Server = _

  def start(port: String, serverRoot: String): Unit = {

    BasicConfigurator.configure()
    initLogging()

    server = new Server()
    val connector = new ServerConnector(server)
    connector.setPort(port.toInt)
    server.addConnector(connector)

    val handler = new ResourceHandler()
    handler.setDirectoriesListed(true)
    handler.setWelcomeFiles(Array[String]("index.html"))
    handler.setResourceBase(serverRoot)
    val handlers = new HandlerList()
    handlers.setHandlers(Array(handler, new DefaultHandler()))

    server.setHandler(handlers)
    server.start()

  }

  def stop() {
    if (server == null) throw new IllegalStateException("Server not Started")

    server.stop()
    server = null
  }

  def initLogging(): Unit = {
    System.setProperty("org.eclipse.jetty.util.log.class", classOf[StdErrLog].getName)
    Logger.getLogger("org.eclipse.jetty").setLevel(Level.ALL)
    Logger.getLogger("org.eclipse.jetty.websocket").setLevel(Level.OFF)
  }

  /*
  Method: getFilesInDirectory
  Description: provides a list of files in the given directory URL.
  @Params: amaNode: Hostname of the URL, port: Port # of the host, directory: destination directory to fetch files
  Note: Should the files in URL root be fetched, provide an empty value to directory.
   */
  def getFilesInDirectory(amaNode: String, port: String, directory: String = ""): Array[String] = {
    println("http://" + amaNode + ":" + port + "/" + directory)
    val html: BufferedSource = Source.fromURL("http://" + amaNode + ":" + port + "/" + directory)
    println(html)
    val htmlDoc = Jsoup.parse(html.mkString)
    val htmlElement: Elements = htmlDoc.body().select("a")
    val files = htmlElement.asScala
    val fileNames = files.map(url => url.attr("href")).filter(file => !file.contains("..")).map(name => name.replace("/", "")).toArray
    fileNames
  }
}