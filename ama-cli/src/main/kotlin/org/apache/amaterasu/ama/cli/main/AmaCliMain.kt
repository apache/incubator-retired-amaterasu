package org.apache.amaterasu.ama.cli.main

import com.xenomachina.argparser.ArgParser
import com.xenomachina.argparser.mainBody
import org.apache.amaterasu.ama.cli.container.ContainerHandler
import org.apache.amaterasu.ama.cli.container.dockerFile


fun main( args : Array<String> ): Unit = mainBody("ama-cli") {
  if (shouldPrintUsage(args)){
    ArgParser(arrayOf("--help")).parseInto( ::AmaCLIArgs )
  }

  val parsedArgs = ArgParser(args).parseInto(::AmaCLIArgs)

  actionToContainer[parsedArgs.agentAction]?.invoke(parsedArgs)



}

private fun createContainerImage(parsedArgs: AmaCLIArgs) {
  dockerFile {
    from(parsedArgs.baseimage)
    commands(parsedArgs.commands)
    entrypoint(parsedArgs.entrypoint)
  }.createImageFile()
}

private fun buildDockerImage(parsedArgs: AmaCLIArgs){
  ContainerHandler(parsedArgs.agentAction)
}

private val actionToContainer = mapOf("CREATE" to ::createContainerImage,
                                              "BUILD" to ::buildDockerImage)

private fun shouldPrintUsage(args: Array<String>) = args.size < AmaCLIArgs.REQUIRED_ARGS

class AmaCLIArgs(parser: ArgParser) {

  companion object {
    const val REQUIRED_ARGS = 4
  }

  val v by parser.flagging("enable verbose mode")

  val baseimage by parser.storing("base docker image")

  val commands by parser.storing("commands to run")

  val entrypoint by parser.storing("entrypoint to the container")

  val agentAction by parser.storing("action to preform [CREATE,BUILD,TAG,PUSH,RUN,COLLECT]")
}

