package org.apache.amaterasu.ama.cli.main

import com.xenomachina.argparser.ArgParser
import com.xenomachina.argparser.mainBody
import org.apache.amaterasu.ama.cli.container.dockerFile


fun main( args : Array<String> ) = mainBody("ama-cli") {

  if (args.size < AmaCLIArgs.REQUIRED_ARGS){
    ArgParser(arrayOf("--help")).parseInto( ::AmaCLIArgs )
  }

  val parsedArgs = ArgParser(args).parseInto(::AmaCLIArgs)
   dockerFile {
      from(parsedArgs.baseimage)
      commands(parsedArgs.commands)
      entrypoint(parsedArgs.entrypoint)
    }.createImageFile()


}

class AmaCLIArgs(parser: ArgParser) {

  companion object {
    const val REQUIRED_ARGS = 3
  }

  val v by parser.flagging("enable verbose mode")

  val baseimage by parser.storing("base docker image")

  val commands by parser.storing("commands to run")

  val entrypoint by parser.storing("entrypoint to the container")
}

