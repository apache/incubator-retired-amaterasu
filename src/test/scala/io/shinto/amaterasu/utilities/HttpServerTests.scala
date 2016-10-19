package io.shinto.amaterasu.utilities

import org.scalatest.{ FlatSpec, Matchers }

import scala.io.Source

/**
  * Created by kirupa on 16/10/16.
  */
class HttpServerTests extends FlatSpec with Matchers {
  "Jetty Web server" should "start HTTP server, serve content and stop successfully" in {
    var data = ""
    try {
      HttpServer.start("8000", getClass.getResource("/").getPath)
      val html = Source.fromURL("http://localhost:8000/jetty-test-data.txt")
      data = html.mkString
    }
    finally {
      HttpServer.stop()
    }
    data should equal("This is a test file to download from Jetty webserver")
  }
}
