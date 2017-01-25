package io.shinto.amaterasu.leader.dsl

import java.io.File

import org.eclipse.jgit.api.Git

import scala.reflect.io.Path

/**
  * The GitUtil class handles getting the job git repository
  */
object GitUtil {

  def cloneRepo(repoAddress: String, branch: String) = {

    val path = Path("repo")
    path.deleteRecursively()

    //TODO: add authentication
    Git.cloneRepository
      .setURI(repoAddress)
      .setDirectory(new File("repo"))
      .setBranch(branch)
      .call

  }

}