package io.shinto.amaterasu.utilities

import java.io.{ FileInputStream, InputStream }
import java.util.Properties

import org.eclipse.jetty.server.{ Server, ServerConnector }
import org.eclipse.jetty.server.handler.{ DefaultHandler, ErrorHandler, HandlerList, ResourceHandler }
import org.eclipse.jetty.servlet.{ DefaultServlet, ServletContextHandler, ServletHolder }
import org.eclipse.jetty.util.thread.QueuedThreadPool

object HttpServer {
  var server: Server = null
  def main(args: Array[String]): Unit = {
    start()

  }
  def start(): Unit = {
    var port: Int = 8000
    var serverRoot: String = "/mesos-dependencies"
    val configFile: InputStream = getClass().getResourceAsStream("target/amaterasu.properties")
    /*val props: Properties = new Properties()
    props.load(configFile)
    configFile.close()

    if (props.containsKey("webserver_port")) port = props.getProperty("webserver_port").toInt
    if (props.containsKey("webserver_root")) serverRoot = props.getProperty("webserver_root")

    val threadPool = new QueuedThreadPool(Runtime.getRuntime.availableProcessors() * 16)
    threadPool.setName("Jetty")*/

    val server = new Server(port)
    //val connector = new ServerConnector(server)
    //connector.setPort(8000)

    // Setup the basic application "context" for this application at "/"
    // This is also known as the handler tree (in jetty speak)
    val context = new ServletContextHandler(ServletContextHandler.SESSIONS)
    context.setResourceBase(serverRoot)
    context.setContextPath("/")
    server.setHandler(context)

    context.setErrorHandler(new ErrorHandler())
    context.setInitParameter("dirAllowed", "true")
    context.setInitParameter("pathInfoOnly", "true")
    context.addServlet(new ServletHolder(new DefaultServlet()), "/")
    server.start()
    server.join()
  }
  def stop() {
    if (server == null) throw new IllegalStateException("Server not started")

    server.stop()
    server.join()
    server = null

    println("Server stopped")
  }
}
