package io.shinto.amaterasu.leader.utilities

/**
  * Created by roadan on 5/16/17.
  */
object ServerTest extends App {
  HttpServer.start("8111", "/ama/dist", "node1")
  println("listening...")
  System.in.read()
  HttpServer.stop()
}
