package io.shinto.amaterasu.utilities

import java.io.File

import io.shinto.amaterasu.leader.utilities.HttpServer
import org.scalatest.{FlatSpec, Matchers}

import scala.io.Source

/**
  * Created by kirupa on 16/10/16.
  */
class HttpServerTests extends FlatSpec with Matchers {

  // this is an ugly hack, getClass.getResource("/").getPath should have worked but
  // stopped working when we moved to gradle :(
  val resources = new File(getClass.getResource("/simple-maki.yml").getPath).getParent

  "Jetty Web server" should "start HTTP server, serve content and stop successfully" in {
    var data = ""
    try {
      HttpServer.start("8000",resources)
      val html = Source.fromURL("http://localhost:8000/jetty-test-data.txt")
      data = html.mkString
    }
    finally {
      HttpServer.stop()
    }
    data should equal("This is a test file to download from Jetty webserver")
  }
}
