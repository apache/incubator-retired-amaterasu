package org.apache.spark.repl.amaterasu

import java.io.PrintWriter

import org.apache.spark.repl.{ SparkIMain, SparkILoop }

import scala.tools.nsc.Settings

/**
  * Created by roadan on 8/13/16.
  */
class AmaSparkILoop(writer: PrintWriter) extends SparkILoop(null, writer) {

  def create = {
    this.createInterpreter
  }

  def setSttings(settings: Settings) = {
    this.settings = settings
  }

  def getIntp: SparkIMain = {
    this.intp
  }

}
