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
package org.apache.amaterasu.executor.execution.actions.runners.spark

import java.io.{ByteArrayOutputStream, File}

import org.apache.amaterasu.common.dataobjects.ExecData
import org.apache.amaterasu.common.execution.actions.Notifier
import org.apache.amaterasu.common.execution.dependencies.Dependencies
import org.apache.amaterasu.sdk.{AmaterasuRunner, RunnersProvider}
import org.apache.spark.repl.amaterasu.runners.spark.{SparkRunnerHelper, SparkScalaRunner}
import org.eclipse.aether.util.artifact.JavaScopes
import org.sonatype.aether.repository.RemoteRepository
import org.sonatype.aether.util.artifact.DefaultArtifact
import com.jcabi.aether.Aether
import org.apache.amaterasu.executor.execution.actions.runners.spark.PySpark.PySparkRunner

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.concurrent.TrieMap

/**
  * Created by roadan on 2/9/17.
  */
class SparkRunnersProvider extends RunnersProvider {

  private val runners = new TrieMap[String, AmaterasuRunner]
  private var conf: Option[Map[String, Any]] = _
  private var executorEnv: Option[Map[String, Any]] = _

  override def init(data: ExecData, jobId: String, outStream: ByteArrayOutputStream, notifier: Notifier, executorId: String): Unit = {

    var jars = Seq.empty[String]

    if (data.deps != null) {
      jars ++= getDependencies(data.deps)
    }

    conf = data.configurations.get("spark")
    executorEnv = data.configurations.get("spark_exec_env")

    val sparkAppName = s"job_${jobId}_executor_$executorId"

    SparkRunnerHelper.notifier = notifier
    val spark = SparkRunnerHelper.createSpark(data.env, sparkAppName, jars, conf, executorEnv)

    val sparkScalaRunner = SparkScalaRunner(data.env, jobId, spark, outStream, notifier, jars)
    sparkScalaRunner.initializeAmaContext(data.env)

    runners.put(sparkScalaRunner.getIdentifier, sparkScalaRunner)

    val pySparkRunner = PySparkRunner(data.env, jobId, notifier, spark, "")
    runners.put(pySparkRunner.getIdentifier(), pySparkRunner)
  }

  override def getGroupIdentifier: String = "spark"

  override def getRunner(id: String): AmaterasuRunner = runners(id)

  private def getDependencies(deps: Dependencies): Seq[String] = {

    // adding a local repo because Aether needs one
    val repo = new File(System.getProperty("java.io.tmpdir"), "ama-repo")

    val remotes = deps.repos.map(r =>
      new RemoteRepository(
        r.id,
        r.`type`,
        r.url
      )).toList.asJava

    val aether = new Aether(remotes, repo)

    deps.artifacts.flatMap(a => {
      aether.resolve(
        new DefaultArtifact(a.groupId, a.artifactId, "", "jar", a.version),
        JavaScopes.RUNTIME
      ).map(a => a)
    }).map(x => x.getFile.getAbsolutePath)

  }
}