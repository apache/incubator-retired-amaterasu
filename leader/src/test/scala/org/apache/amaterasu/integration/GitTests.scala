/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.amaterasu.integration

import org.apache.amaterasu.leader.dsl.GitUtil
import org.scalatest.{FlatSpec, Matchers}

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
