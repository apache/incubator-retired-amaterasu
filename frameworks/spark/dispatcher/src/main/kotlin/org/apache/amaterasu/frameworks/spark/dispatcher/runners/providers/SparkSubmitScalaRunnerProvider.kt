package org.apache.amaterasu.frameworks.spark.dispatcher.runners.providers

import com.uchuhimo.konf.Config
import org.apache.amaterasu.common.configuration.ClusterConfig
import org.apache.amaterasu.common.dataobjects.ActionData
import org.apache.amaterasu.common.utils.ArtifactUtil
import org.apache.amaterasu.leader.common.configuration.Job
import org.apache.amaterasu.sdk.frameworks.RunnerSetupProvider

class SparkSubmitScalaRunnerProvider(val conf: ClusterConfig) : RunnerSetupProvider() {

    override fun getCommand(jobId: String, actionData: ActionData, env: Config, executorId: String, callbackAddress: String): String {
        val util = ArtifactUtil(listOf(actionData.repo), jobId)
        val classParam = if (actionData.hasArtifact) " --class ${actionData.entryClass}" else ""

        return "\$SPARK_HOME/bin/spark-submit $classParam ${util.getLocalArtifacts(actionData.artifact).first().name} " +
                " --master ${env[Job.master]}" +
                " --jars spark-runtime-${conf.version()}.jar >&1"
    }

    override fun getActionUserResources(jobId: String, actionData: ActionData): Array<String> = arrayOf()

    override fun getActionDependencies(jobId: String, actionData: ActionData): Array<String> = arrayOf()

    override val hasExecutor: Boolean = false
    override val runnerResources: Array<String> = arrayOf()

}