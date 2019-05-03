/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.amaterasu.common.utils

import com.jcabi.aether.Aether
import org.apache.amaterasu.common.dataobjects.Artifact
import org.apache.amaterasu.common.dataobjects.Repo
import org.sonatype.aether.repository.RemoteRepository
import org.sonatype.aether.util.artifact.DefaultArtifact
import org.sonatype.aether.util.artifact.JavaScopes
import java.io.File

class ArtifactUtil(repos: List<Repo> = listOf(), jobId: String) {

    private var repo: File
    private val remoteRepos: MutableList<RemoteRepository> = mutableListOf()

    init {
        val jarFile = File(this.javaClass.protectionDomain.codeSource.location.path)
        val amaHome = File(jarFile.parent).parent
        repo = File("$amaHome/dist/$jobId")

        addRepos(repos)
    }

    fun addRepo(repo: Repo) {
        remoteRepos.add(RemoteRepository(repo.id, repo.type, repo.url))
    }

    fun addRepos(repos: List<Repo>) {
        repos.forEach { addRepo(it) }
    }

    fun getLocalArtifacts(artifact: Artifact): List<File> {

        val aether = Aether(remoteRepos, repo)
        val resolvedArtifacts = aether.resolve(DefaultArtifact(artifact.groupId, artifact.artifactId, "", "jar", artifact.version),
                JavaScopes.RUNTIME)

        return resolvedArtifacts.map { it.file }
    }
}