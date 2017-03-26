package io.shinto.amaterasu.integration

import io.shinto.amaterasu.leader.dsl.GitUtil
import org.scalatest.{ Matchers, FlatSpec }
import scala.reflect.io.Path

class GitTests extends FlatSpec with Matchers {

  "GitUtil.cloneRepo" should "clone the sample job git repo" in {

    val path = Path("repo")
    path.deleteRecursively()

    GitUtil.cloneRepo("https://github.com/shintoio/amaterasu-job-sample.git", "master")

    val exists = new java.io.File("repo/maki.yml").exists
    exists should be(true)
  }
}
