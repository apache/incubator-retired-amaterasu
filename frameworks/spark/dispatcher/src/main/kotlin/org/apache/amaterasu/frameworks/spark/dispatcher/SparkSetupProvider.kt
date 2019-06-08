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
package org.apache.amaterasu.frameworks.spark.dispatcher

import org.apache.amaterasu.common.configuration.ClusterConfig
import org.apache.amaterasu.common.configuration.ConfigManager
import org.apache.amaterasu.frameworks.spark.dispatcher.runners.providers.PySparkRunnerProvider
import org.apache.amaterasu.frameworks.spark.dispatcher.runners.providers.SparkSubmitScalaRunnerProvider
import org.apache.amaterasu.leader.common.utilities.MemoryFormatParser
import org.apache.amaterasu.sdk.frameworks.FrameworkSetupProvider
import org.apache.amaterasu.sdk.frameworks.RunnerSetupProvider
import org.apache.amaterasu.sdk.frameworks.configuration.DriverConfiguration
import org.apache.commons.lang.StringUtils
import java.io.File

class SparkSetupProvider : FrameworkSetupProvider {

    private lateinit var env: String
    private lateinit var conf: ClusterConfig

    private lateinit var runnerProviders: Map<String, RunnerSetupProvider>

    override fun init(env: String, conf: ClusterConfig) {

        this.env = env
        this.conf = conf

        runnerProviders = mapOf(
                "jar" to SparkSubmitScalaRunnerProvider(conf),
                "pyspark" to PySparkRunnerProvider(env, conf)
        )

    }

    override val environmentVariables: Map<String, String> by lazy {
        when (conf.mode()) {
            "mesos" -> mapOf("SPARK_HOME" to "spark-${conf.webserver().sparkVersion()}", "SPARK_HOME_DOCKER" to "/opt/spark/")
            "yarn" -> mapOf("SPARK_HOME" to StringUtils.stripStart(conf.spark().home(), "/"))
            else -> mapOf()
        }
    }

    override val groupResources: Array<File> by lazy {
        when (conf.mode()) {
            "mesos" -> arrayOf(File("spark-${conf.webserver().sparkVersion()}.tgz"), File("spark-runner-${conf.version()}-all.jar"), File("spark-runtime-${conf.version()}.jar"))
            "yarn" -> arrayOf(File("spark-runner-${conf.version()}-all.jar"), File("spark-runtime-${conf.version()}.jar"), File("executor-${conf.version()}-all.jar"), File(conf.spark().home()))
            else -> arrayOf()
        }
    }

    override fun getRunnerProvider(runnerId: String): RunnerSetupProvider {
        return runnerProviders.getValue(runnerId)
    }

    override val groupIdentifier: String = "spark"
    override val configurationItems = arrayOf("sparkProperties", "sparkOptions")

    override fun getDriverConfiguration(configManager: ConfigManager): DriverConfiguration {
        val sparkOptions: Map<String, Any> = configManager.config["sparkOptions"]
        val sparkProperties: Map<String, Any> = configManager.config["sparkProperties"]

        val cpu: Int = when {
            sparkOptions.containsKey("driver-cores") -> sparkOptions["driver-cores"].toString().toInt()
            sparkProperties.containsKey("spark.yarn.am.cores") -> sparkProperties["spark.yarn.am.cores"].toString().toInt()
            sparkProperties.containsKey("spark.driver.cores") -> sparkProperties["spark.driver.cores"].toString().toInt()
            else -> 1
        }

        val mem: Int = when {
            sparkOptions.containsKey("driver-memory") -> MemoryFormatParser.extractMegabytes(sparkOptions["driver-memory"].toString())
            sparkProperties.containsKey("spark.yarn.am.memory") -> MemoryFormatParser.extractMegabytes(sparkProperties["spark.yarn.am.memory"].toString())
            sparkProperties.containsKey("spark.driver.memeory") -> MemoryFormatParser.extractMegabytes(sparkProperties["spark.driver.memeory"].toString())
            else -> 1024
        }
        return DriverConfiguration(mem, cpu)
    }
}