package org.apache.amaterasu.ama.cli.container

import com.github.dockerjava.api.DockerClient
import com.github.dockerjava.core.DefaultDockerClientConfig
import com.github.dockerjava.core.DockerClientBuilder
import com.github.dockerjava.jaxrs.JerseyDockerCmdExecFactory


class ContainerHandler {

  private val dockerHost = System.getenv("DOCKER_HOST") ?: "unix:///var/run/docker.sock"
  private val dockerRegistry = System.getenv("DOCKER_REGISTRY") ?: "127.0.0.1:5000"


  private fun getDockerClient(config: DefaultDockerClientConfig, dockerCmdExecFactory: JerseyDockerCmdExecFactory?): DockerClient {
    return DockerClientBuilder.getInstance(config)
        .withDockerCmdExecFactory(dockerCmdExecFactory)
        .build()
  }


  private fun buildDockerServerExecutor() : JerseyDockerCmdExecFactory {
    return JerseyDockerCmdExecFactory()
        .withReadTimeout(1000)
        .withConnectTimeout(1000)
        .withMaxTotalConnections(100)
        .withMaxPerRouteConnections(10)
  }

  private fun defaultContainerHandler()  {
    DefaultDockerClientConfig.createDefaultConfigBuilder()
        .withDockerHost(dockerHost)
        .withDockerTlsVerify(false)
        .withRegistryUrl(dockerRegistry)
        .build()
  }

}