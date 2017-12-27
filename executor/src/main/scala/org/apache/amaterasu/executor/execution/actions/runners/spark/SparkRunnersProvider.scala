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

import java.io.{ByteArrayOutputStream, File, PrintWriter, StringWriter}

import com.jcabi.aether.Aether
import org.apache.amaterasu.common.dataobjects.ExecData
import org.apache.amaterasu.common.execution.actions.Notifier
import org.apache.amaterasu.common.execution.dependencies.{Dependencies, PythonDependencies, PythonPackage}
import org.apache.amaterasu.common.logging.Logging
import org.apache.amaterasu.executor.execution.actions.runners.spark.PySpark.PySparkRunner
import org.apache.amaterasu.sdk.{AmaterasuRunner, RunnersProvider}
import org.apache.spark.repl.amaterasu.runners.spark.{SparkRunnerHelper, SparkScalaRunner}
import org.eclipse.aether.util.artifact.JavaScopes
import org.sonatype.aether.repository.RemoteRepository
import org.sonatype.aether.util.artifact.DefaultArtifact

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.concurrent.TrieMap
import scala.sys.process._

class SparkRunnersProvider extends RunnersProvider with Logging {

  private val runners = new TrieMap[String, AmaterasuRunner]
  private var shellLoger = ProcessLogger(
    (o: String) => log.info(o),
    (e: String) => log.error(e)

  )
  private var conf: Option[Map[String, Any]] = _
  private var executorEnv: Option[Map[String, Any]] = _

  override def init(data: ExecData, jobId: String, outStream: ByteArrayOutputStream, notifier: Notifier, executorId: String,
                    propFile: String): Unit = {
    shellLoger = ProcessLogger(
      (o: String) => log.info(o),
      (e: String) => log.error("", e)
    )

    var jars = Seq.empty[String]

    if (data.deps != null) {
      jars ++= getDependencies(data.deps)
    }

    if (data.pyDeps != null &&
        data.pyDeps.packages.nonEmpty) {
      loadPythonDependencies(data.pyDeps, notifier)
    }

    conf = data.configurations.get("spark")
    executorEnv = data.configurations.get("spark_exec_env")
    val sparkAppName = s"job_${jobId}_executor_$executorId"

    SparkRunnerHelper.notifier = notifier
    val spark = SparkRunnerHelper.createSpark(data.env, sparkAppName, jars, conf, executorEnv, propFile)

    lazy val sparkScalaRunner = SparkScalaRunner(data.env, jobId, spark, outStream, notifier, jars)
    sparkScalaRunner.initializeAmaContext(data.env)

    runners.put(sparkScalaRunner.getIdentifier, sparkScalaRunner)
    // TODO: get rid of hard-coded version
    lazy val pySparkRunner = PySparkRunner(data.env, jobId, notifier, spark, "spark-2.1.1-bin-hadoop2.7/python:spark-2.1.1-bin-hadoop2.7/python/pyspark:spark-2.1.1-bin-hadoop2.7/python/pyspark/build:spark-2.1.1-bin-hadoop2.7/python/pyspark/lib/py4j-0.10.4-src.zip", data.pyDeps)
    runners.put(pySparkRunner.getIdentifier(), pySparkRunner)
  }

  private def installAnacondaPackage(pythonPackage: PythonPackage): Unit = {
    val channel = pythonPackage.channel.getOrElse("anaconda")
    if (channel == "anaconda") {
      Seq("bash", "-c", s"$$PWD/miniconda/bin/python -m conda install -y ${pythonPackage.packageId}") ! shellLoger
    } else {
      Seq("bash", "-c", s"$$PWD/miniconda/bin/python -m conda install -y -c $channel ${pythonPackage.packageId}") ! shellLoger
    }
  }

  private def installAnacondaOnNode(): Unit = {
    // TODO: get rid of hard-coded version
    Seq("bash", "-c", "sh Miniconda2-latest-Linux-x86_64.sh -b -p $PWD/miniconda") ! shellLoger
    Seq("bash", "-c", "$PWD/miniconda/bin/python -m conda install -y conda-build") ! shellLoger
    Seq("bash", "-c", "ln -s $PWD/spark-2.1.1-bin-hadoop2.7/python/pyspark $PWD/miniconda/pkgs/pyspark") ! shellLoger
  }

  private def loadPythonDependencies(deps: PythonDependencies, notifier: Notifier): Unit = {
    notifier.info("loading anaconda evn")
    installAnacondaOnNode()
    val codegenPackage = PythonPackage("codegen", channel = Option("auto"))
    installAnacondaPackage(codegenPackage)
    try {
      // notifier.info("loadPythonDependencies #5")
      deps.packages.foreach(pack => {
        pack.index.getOrElse("anaconda").toLowerCase match {
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