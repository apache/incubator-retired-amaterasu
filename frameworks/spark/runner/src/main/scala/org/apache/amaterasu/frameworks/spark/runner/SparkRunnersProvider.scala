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
package org.apache.amaterasu.frameworks.spark.runner

import java.io._

import com.jcabi.aether.Aether
import org.apache.amaterasu.common.configuration.ClusterConfig
import org.apache.amaterasu.common.dataobjects.ExecData
import org.apache.amaterasu.common.execution.actions.Notifier
import org.apache.amaterasu.common.execution.dependencise.{Dependencies, PythonDependencies, PythonPackage}
import org.apache.amaterasu.common.logging.Logging
import org.apache.amaterasu.frameworks.spark.runner.sparksql.SparkSqlRunner
import org.apache.amaterasu.frameworks.spark.runner.pyspark.PySparkRunner
import org.apache.amaterasu.frameworks.spark.runner.repl.{SparkRunnerHelper, SparkScalaRunner}
import org.apache.amaterasu.sdk.{AmaterasuRunner, RunnersProvider}
import org.eclipse.aether.util.artifact.JavaScopes
import org.sonatype.aether.repository.RemoteRepository
import org.sonatype.aether.util.artifact.DefaultArtifact

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.concurrent.TrieMap
import scala.sys.process._

class SparkRunnersProvider extends Logging with RunnersProvider {

  private val runners = new TrieMap[String, AmaterasuRunner]
  private var shellLoger = ProcessLogger(
    (o: String) => log.info(o),
    (e: String) => log.error(e)

  )
  private var conf: Map[String, AnyRef] = _
  private var executorEnv: Map[String, AnyRef] = _
  private var clusterConfig: ClusterConfig = _

  override def init(execData: ExecData,
                    jobId: String,
                    outStream: ByteArrayOutputStream,
                    notifier: Notifier,
                    executorId: String,
                    config: ClusterConfig,
                    hostName: String): Unit = {

    shellLoger = ProcessLogger(
      (o: String) => log.info(o),
      (e: String) => log.error("", e)
    )
    clusterConfig = config
    var jars = Seq.empty[String]

    if (execData.getDeps != null) {
      jars ++= getDependencies(execData.getDeps)
    }

    if (execData.getPyDeps != null &&
      execData.getPyDeps.getPackages.nonEmpty) {
      loadPythonDependencies(execData.getPyDeps, notifier)
    }

    conf = execData.getConfigurations.get("spark").toMap
    executorEnv = execData.getConfigurations.get("spark_exec_env").toMap
    val sparkAppName = s"job_${jobId}_executor_$executorId"

    SparkRunnerHelper.notifier = notifier
    val spark = SparkRunnerHelper.createSpark(execData.getEnv, sparkAppName, jars, Some(conf), Some(executorEnv), config, hostName)

    lazy val sparkScalaRunner = SparkScalaRunner(execData.getEnv, jobId, spark, outStream, notifier, jars)
    sparkScalaRunner.initializeAmaContext(execData.getEnv)

    runners.put(sparkScalaRunner.getIdentifier, sparkScalaRunner)
    var pypath = ""
    // TODO: get rid of hard-coded version
    config.mode match {
      case "yarn" =>
        pypath = s"$$PYTHONPATH:$$SPARK_HOME/python:$$SPARK_HOME/python/build:${config.spark.home}/python:${config.spark.home}/python/pyspark:${config.spark.home}/python/pyspark/build:${config.spark.home}/python/pyspark/lib/py4j-0.10.4-src.zip:${new File(".").getAbsolutePath}"
      case "mesos" =>
        pypath = s"${new File(".").getAbsolutePath}/miniconda/pkgs:${new File(".").getAbsolutePath}"
    }
    lazy val pySparkRunner = PySparkRunner(execData.getEnv, jobId, notifier, spark, pypath, execData.getPyDeps, config)
    runners.put(pySparkRunner.getIdentifier, pySparkRunner)

    lazy val sparkSqlRunner = SparkSqlRunner(execData.getEnv, jobId, notifier, spark)
    runners.put(sparkSqlRunner.getIdentifier, sparkSqlRunner)
  }

  private def installAnacondaPackage(pythonPackage: PythonPackage): Unit = {
    val channel = pythonPackage.getChannel
    if (channel == "anaconda") {
      Seq("bash", "-c", s"export HOME=$$PWD && ./miniconda/bin/python -m conda install -y ${pythonPackage.getPackageId}") ! shellLoger
    } else {
      Seq("bash", "-c", s"export HOME=$$PWD && ./miniconda/bin/python -m conda install -y -c $channel ${pythonPackage.getPackageId}") ! shellLoger
    }
  }

  private def installAnacondaOnNode(): Unit = {
    // TODO: get rid of hard-coded version

    this.clusterConfig.mode match {
      case "yarn" => Seq("sh", "-c", "export HOME=$PWD && ./miniconda.sh -b -p miniconda") ! shellLoger
      case "mesos" => Seq("sh", "miniconda.sh", "-b", "-p", "miniconda") ! shellLoger
    }

    Seq("bash", "-c", "export HOME=$PWD && ./miniconda/bin/python -m conda install -y conda-build") ! shellLoger
    Seq("bash", "-c", "ln -s spark/python/pyspark miniconda/pkgs/pyspark") ! shellLoger
  }

  private def loadPythonDependencies(deps: PythonDependencies, notifier: Notifier): Unit = {
    notifier.info("loading anaconda evn")
    installAnacondaOnNode()
    val codegenPackage = new PythonPackage( "codegen", "", "auto")
    installAnacondaPackage(codegenPackage)
    try {
      // notifier.info("loadPythonDependencies #5")
      deps.getPackages.foreach(pack => {
        pack.getIndex.toLowerCase match {
          case "anaconda" => installAnacondaPackage(pack)
          // case "pypi" => installPyPiPackage(pack)
        }
      })
    }
    catch {

      case rte: RuntimeException =>
        val sw = new StringWriter
        rte.printStackTrace(new PrintWriter(sw))
        notifier.error("", s"Failed to activate environment (runtime) - cause: ${rte.getCause}, message: ${rte.getMessage}, Stack: \n${sw.toString}")
      case e: Exception =>
        val sw = new StringWriter
        e.printStackTrace(new PrintWriter(sw))
        notifier.error("", s"Failed to activate environment (other) - type: ${e.getClass.getName}, cause: ${e.getCause}, message: ${e.getMessage}, Stack: \n${sw.toString}")
    }
  }

  override def getGroupIdentifier: String = "spark"

  override def getRunner(id: String): AmaterasuRunner = runners(id)

  private def getDependencies(deps: Dependencies): Seq[String] = {

    // adding a local repo because Aether needs one
    val repo = new File(System.getProperty("java.io.tmpdir"), "ama-repo")

    val remotes = deps.getRepos.map(r =>
      new RemoteRepository(
        r.getId,
        r.getType,
        r.getUrl
      )).toList.asJava

    val aether = new Aether(remotes, repo)

    deps.getArtifacts.flatMap(a => {
      aether.resolve(
        new DefaultArtifact(a.getGroupId, a.getArtifactId, "", "jar", a.getVersion),
        JavaScopes.RUNTIME
      ).map(a => a)
    }).map(x => x.getFile.getAbsolutePath)

  }
}