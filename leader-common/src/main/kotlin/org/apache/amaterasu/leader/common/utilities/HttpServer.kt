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

import org.apache.log4j.BasicConfigurator
import org.apache.log4j.Level
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.server.ServerConnector
import org.eclipse.jetty.server.handler.DefaultHandler
import org.eclipse.jetty.server.handler.HandlerList
import org.eclipse.jetty.server.handler.ResourceHandler
import org.jsoup.Jsoup
import org.jsoup.select.Elements
import org.apache.log4j.Logger
import org.eclipse.jetty.util.log.StdErrLog

class HttpServer {
    lateinit var server: Server

    fun start(port: String, serverRoot: String) {
        BasicConfigurator.configure()
        initLogging()

        server = Server()
        val connector = ServerConnector(server)
        connector.port = port.toInt()
        server.addConnector(connector)

        val handler = ResourceHandler()
        handler.isDirectoriesListed = true
        handler.welcomeFiles = arrayOf("index.html")
        handler.resourceBase = serverRoot
        val handlers = HandlerList()
        handlers.handlers = arrayOf(handler, DefaultHandler())

        server.handler = handlers
        server.start()
    }

    private fun initLogging() {
        System.setProperty("org.eclipse.jetty.util.log.class", StdErrLog::javaClass.name)
        Logger.getLogger("org.eclipse.jetty").level = Level.ALL
        Logger.getLogger("org.eclipse.jetty.websocket").level = Level.OFF
    }

    /**
     * Method: getFilesInDirectory
     * Description: provides a list of files in the given directory URL.
     * @Params: amaNode: Hostname of the URL, port: Port # of the host, directory: destination directory to fetch files
     * Note: Should the files in URL root be fetched, provide an empty value to directory.
     */
    fun getFilesInDirectory(amaNode: String, port: String, directory: String = ""): Array<String> {

        val htmlDoc = Jsoup.connect("http://$amaNode:$port/$directory").get()
        val files: Elements = htmlDoc.body().select("a")
        return files.map { it.attr("href") }
                .filter { !it.contains("..") }
                .map { it.replace("/", "") }.toTypedArray()
    }

    fun stop() {
        if (::server.isInitialized) {
            server.stop()
        }
    }

}