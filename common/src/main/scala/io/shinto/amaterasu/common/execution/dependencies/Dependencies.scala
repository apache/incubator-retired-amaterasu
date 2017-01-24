package io.shinto.amaterasu.common.execution.dependencies

import scala.collection.mutable.ListBuffer

/**
  * Created by roadan on 8/28/16.
  */
case class Dependencies(repos: ListBuffer[Repo], artifacts: List[Artifact])
case class Repo(id: String, `type`: String, url: String)
case class Artifact(groupId: String, artifactId: String, version: String)