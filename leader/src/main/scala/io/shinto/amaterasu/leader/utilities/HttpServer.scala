package io.shinto.amaterasu.leader.utilities

import io.shinto.amaterasu.common.logging.Logging
import org.apache.log4j.{BasicConfigurator, Level, Logger}
import org.eclipse.jetty.server.handler.ErrorHandler
import org.eclipse.jetty.server.{Server, ServerConnector}
import org.eclipse.jetty.servlet.{DefaultServlet, ServletContextHandler, ServletHolder}
import org.eclipse.jetty.util.log.StdErrLog
/**
  * Created by kirupa
  * Implementation of Jetty Web server to server Amaterasu libraries and other distribution files
  */
object HttpServer extends Logging {
  val logger = Logger.getLogger(HttpServer.getClass)
  var server: Server = null

  def start(port: String, serverRoot: String): Unit = {

    /*val threadPool = new QueuedThreadPool(Runtime.getRuntime.availableProcessors() * 16)
    threadPool.setName("Jetty")*/
    BasicConfigurator.configure()
    initLogging()
    server = new Server()
    val connector = new ServerConnector(server)
    connector.setPort(port.toInt)
    server.addConnector(connector)
    val context = new ServletContextHandler(ServletContextHandler.SESSIONS)
    context.setResourceBase(serverRoot)
    context.setContextPath("/")
    server.setHandler(context)
    context.setErrorHandler(new ErrorHandler())
    context.setInitParameter("dirAllowed", "true")
    context.setInitParameter("pathInfoOnly", "true")
    context.addServlet(new ServletHolder(new DefaultServlet()), "/")
    server.start()
  }

  def stop() {
    if (server == null) throw new IllegalStateException("Server not started")

    server.stop()
    server = null
  }

  def initLogging(): Unit = {
    System.setProperty("org.eclipse.jetty.util.log.class", classOf[StdErrLog].getName)
    Logger.getLogger("org.eclipse.jetty").setLevel(Level.OFF)
    Logger.getLogger("org.eclipse.jetty.websocket").setLevel(Level.OFF)
  }
}
