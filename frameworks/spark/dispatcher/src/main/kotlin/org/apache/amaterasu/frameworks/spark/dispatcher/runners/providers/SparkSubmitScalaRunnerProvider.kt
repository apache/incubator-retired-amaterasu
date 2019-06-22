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
package org.apache.amaterasu.frameworks.spark.dispatcher.runners.providers

import com.uchuhimo.konf.Config
import org.apache.amaterasu.common.configuration.ClusterConfig
import org.apache.amaterasu.common.dataobjects.ActionData
import org.apache.amaterasu.common.utils.ArtifactUtil
import org.apache.amaterasu.common.configuration.Job
import org.apache.amaterasu.sdk.frameworks.RunnerSetupProvider


class SparkSubmitScalaRunnerProvider(val conf: ClusterConfig) : RunnerSetupProvider() {

    override fun getCommand(jobId: String, actionData: ActionData, env: Config, executorId: String, callbackAddress: String): String {
        val util = ArtifactUtil(listOf(actionData.repo), jobId)
        val classParam = if (actionData.hasArtifact) " --class ${actionData.entryClass}" else ""
        val sparkProperties: Map<String, Any> = env["sparkProperties"]
        val sparkOptions: Map<String, Any> = env["sparkOptions"]
        val master = if (!sparkOptions.containsKey("master")) " --master ${env[Job.master]} " else ""

        return "\$SPARK_HOME/bin/spark-submit $classParam ${util.getLocalArtifacts(actionData.artifact).first().name} " +
                SparkCommandLineHelper.getOptions(sparkOptions) + " " +
                SparkCommandLineHelper.getProperties(sparkProperties) + " " +
                master +
                " --jars spark-runtime-${conf.version()}-all.jar >&1"
    }

    override fun getActionUserResources(jobId: String, actionData: ActionData): Array<String> = arrayOf()

    override fun getActionDependencies(jobId: String, actionData: ActionData): Array<String> = arrayOf()

    override val hasExecutor: Boolean = false

    override val runnerResources: Array<String> = arrayOf()

}