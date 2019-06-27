package org.apache.amaterasu.ama.cli.container

import com.github.dockerjava.api.DockerClient
import com.github.dockerjava.api.model.BuildResponseItem
import com.github.dockerjava.core.DefaultDockerClientConfig
import com.github.dockerjava.core.DockerClientBuilder
import com.github.dockerjava.core.command.BuildImageResultCallback
import com.github.dockerjava.core.command.PushImageResultCallback
import com.github.dockerjava.jaxrs.JerseyDockerCmdExecFactory
import mu.KLogging
import java.io.File


class ContainerHandler(val action : String, val imageName : String = "") {
  companion object: KLogging()

  private val dockerHost = System.getenv("DOCKER_HOST") ?: "unix:///var/run/docker.sock"
  private val dockerRegistry = System.getenv("DOCKER_REGISTRY") ?: "127.0.0.1:5000"

  private val actionHandlers = mapOf("BUILD" to ::buildDockerImage,
                    "PUSH" to ::pushDockerImageToRegistry)

  init {
    logger.info("Got $action for docker as action")
    val dockerExec =  buildDockerServerExecutor()
    val dockerClient = getDockerClient(defaultContainerHandler(), dockerExec)
    actionHandlers[action]?.invoke(dockerClient)

  }


  private fun pushDockerImageToRegistry(dockerClient: DockerClient){
    dockerClient.pushImageCmd("$dockerRegistry/$imageName").exec(PushImageResultCallback()).awaitCompletion()
  }

  private fun tagDockerImage(dockerClient: DockerClient, imageId : String) {
    dockerClient.tagImageCmd(imageId,"$dockerRegistry/$imageName", imageName).exec()
  }

  private fun buildDockerImage(dockerClient: DockerClient) {
    val baseDir = File("./")
    val imageId = dockerClient.buildImageCmd(baseDir).exec(BuildImageCallback()).awaitImageId()
    tagDockerImage(dockerClient,imageId)
  }

  private fun buildDockerServerExecutor() : JerseyDockerCmdExecFactory {
    return JerseyDockerCmdExecFactory()
        .withReadTimeout(1000)
        .withConnectTimeout(1000)
        .withMaxTotalConnections(100)
        .withMaxPerRouteConnections(10)
  }

  private fun defaultContainerHandler(): DefaultDockerClientConfig {
    return DefaultDockerClientConfig.createDefaultConfigBuilder()
        .withDockerHost(dockerHost)
        .withDockerTlsVerify(false)
        .withRegistryUrl(dockerRegistry)
        .build()
  }

  private fun getDockerClient(config: DefaultDockerClientConfig, dockerCmdExecFactory: JerseyDockerCmdExecFactory?): DockerClient {
    return DockerClientBuilder.getInstance(config)
        .withDockerCmdExecFactory(dockerCmdExecFactory)
        .build()
  }




  private class BuildImageCallback : BuildImageResultCallback() {
    override fun onNext(item: BuildResponseItem) {
      super.onNext(item)
    }
  }





}