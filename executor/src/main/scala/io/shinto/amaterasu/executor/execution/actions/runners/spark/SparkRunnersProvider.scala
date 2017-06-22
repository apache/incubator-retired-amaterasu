package io.shinto.amaterasu.executor.execution.actions.runners.spark

import java.io.{ByteArrayOutputStream, File}

import io.shinto.amaterasu.common.dataobjects.ExecData
import io.shinto.amaterasu.common.execution.actions.Notifier
import io.shinto.amaterasu.common.execution.dependencies.Dependencies
import io.shinto.amaterasu.sdk.{AmaterasuRunner, RunnersProvider}
import io.shinto.amaterasu.executor.execution.actions.runners.spark.PySpark.PySparkRunner

import org.apache.spark.repl.amaterasu.runners.spark.{SparkRunnerHelper, SparkScalaRunner}

import org.eclipse.aether.util.artifact.JavaScopes
import org.sonatype.aether.repository.RemoteRepository
import org.sonatype.aether.util.artifact.DefaultArtifact

import com.jcabi.aether.Aether

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.concurrent.TrieMap

/**
  * Created by roadan on 2/9/17.
  */
class SparkRunnersProvider extends RunnersProvider {

  private val runners = new TrieMap[String, AmaterasuRunner]
  private var conf: Map[String, Any] = _
  private var executorEnv: Map[String, Any] = _

  override def init(data: ExecData, jobId: String, outStream: ByteArrayOutputStream, notifier: Notifier, executorId: String): Unit = {

    var jars = Seq.empty[String]

    if (data.deps != null) {
      jars ++= getDependencies(data.deps)
    }

    conf = data.configurations("spark")
    executorEnv = data.configurations("spark_exec")

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