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
import org.apache.amaterasu.frameworks.python.dispatcher.runners.providers.PythonRunnerProviderBase
import org.apache.amaterasu.common.configuration.Job

class PySparkRunnerProvider(env: String, conf: ClusterConfig) : PythonRunnerProviderBase(env, conf) {

    override fun getCommand(jobId: String, actionData: ActionData, env: Config, executorId: String, callbackAddress: String): String {
        val sparkProperties: Map<String, Any> = env["sparkProperties"]
        val sparkOptions: Map<String, Any> = env["sparkOptions"]
        val command = super.getCommand(jobId, actionData, env, executorId, callbackAddress)
        val hive  = if (conf.mode() == "yarn") "--files \$SPARK_HOME/conf/hive-site.xml " else ""
        val master = if (!sparkOptions.containsKey("master")) " --master ${env[Job.master]} " else ""

        return "$command && \$SPARK_HOME/bin/spark-submit $master " +
                SparkCommandLineHelper.getOptions(sparkOptions) + " " +
                SparkCommandLineHelper.getProperties(sparkProperties) + " " +
                "--conf spark.pyspark.python=${conf.pythonPath()} " +
                "--conf spark.pyspark.driver.python=$virtualPythonPath " +
                hive +
                " ${actionData.src}"
    }

    override fun getActionUserResources(jobId: String, actionData: ActionData): Array<String> = arrayOf()

    override val runnerResources: Array<String>
        get() {
            var resources = super.runnerResources
            resources += "amaterasu_pyspark-${conf.version()}.zip"
            //log.info("PYSPARK RESOURCES ==> ${resources.toSet}")
            return resources
        }

    override val hasExecutor: Boolean = false
}