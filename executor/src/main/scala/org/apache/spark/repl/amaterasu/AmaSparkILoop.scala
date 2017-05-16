package org.apache.spark.repl.amaterasu

import java.io.PrintWriter

import org.apache.spark.repl.SparkILoop

import scala.tools.nsc.Settings
import scala.tools.nsc.interpreter.IMain

/**
  * Created by roadan on 8/13/16.
  */
class AmaSparkILoop(writer: PrintWriter) extends SparkILoop(None, writer) {

  def create = {
    this.createInterpreter
  }

  def setSttings(settings: Settings) = {
    this.settings = settings
  }

  def getIntp: IMain = {
    this.intp
  }

}
