package org.apache.amaterasu.leader.container



fun dockerFile(initilizer: DockerImageFile.() -> Unit) : DockerImageFile {
  return DockerImageFile().apply(initilizer)
}

class DockerImageFile {
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

  fun compileImage() : String {
    return "FROM " + baseImage + "\n" +
        commandsList.joinToString (separator = "\n") + "\n" +
      "ENTRYPOINT " + entrypoint
  }
}
