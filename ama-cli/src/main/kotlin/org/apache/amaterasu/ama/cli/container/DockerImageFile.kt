package org.apache.amaterasu.ama.cli.container

import java.io.File


fun dockerFile(initilizer: DockerImageFile.() -> Unit) : DockerImageFile {
  return DockerImageFile().apply(initilizer)
}

class DockerImageFile {
  val DOCKER_FILE_NAME = "Dockerfile"
  var baseImage : String = ""
  var commandsList : MutableList<String> = mutableListOf()
  var entrypoint  = mutableListOf<String>()
  fun from(baseImage : String) {
    this.baseImage = baseImage
  }

  fun commands(vararg commands : String){
    commandsList.addAll(commands)
  }

  fun entrypoint(vararg command : String) {
    entrypoint.addAll(command)
  }

  fun createImageFile(dockerFileName : String = DOCKER_FILE_NAME) {
    File(dockerFileName).writeText(compileImageString())
  }

  private fun compileImageString() : String {
    return "FROM " + baseImage + "\n" +
        commandsList.joinToString (separator = "\n") + "\n" +
      "ENTRYPOINT " + entrypoint
  }
}
