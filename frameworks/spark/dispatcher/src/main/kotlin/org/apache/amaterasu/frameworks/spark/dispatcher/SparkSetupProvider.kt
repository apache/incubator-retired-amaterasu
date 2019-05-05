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
import org.apache.amaterasu.frameworks.spark.dispatcher.runners.providers.PySparkRunnerProvider
import org.apache.amaterasu.frameworks.spark.dispatcher.runners.providers.SparkSubmitScalaRunnerProvider
import org.apache.amaterasu.leader.common.utilities.DataLoader
import org.apache.amaterasu.leader.common.utilities.MemoryFormatParser
import org.apache.amaterasu.sdk.frameworks.FrameworkSetupProvider
import org.apache.amaterasu.sdk.frameworks.RunnerSetupProvider
import org.apache.amaterasu.sdk.frameworks.configuration.DriverConfiguration
import org.apache.commons.lang.StringUtils
import java.io.File

class SparkSetupProvider : FrameworkSetupProvider {

    private lateinit var env: String
    private lateinit var conf: ClusterConfig

    private val sparkExecConfigurations: Map<String, Any> by lazy {
        val execData = DataLoader.getExecutorData(env, conf)
        execData.configurations["spark"].orEmpty()
    }

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
    override val configurationItems = arrayOf("sparkConfiguration", "sparkExecutor")

    override val driverConfiguration: DriverConfiguration
        get() {
            //TODO: Spark configuration sould come for the ENV only
            val sparkOpts = conf.spark().opts()
            val cpu: Int = when {
                sparkExecConfigurations.containsKey("spark.yarn.am.cores") -> sparkExecConfigurations["spark.yarn.am.cores"].toString().toInt()
                sparkExecConfigurations.containsKey("spark.driver.cores") -> sparkExecConfigurations["spark.driver.cores"].toString().toInt()
                sparkOpts.contains("yarn.am.cores") -> sparkOpts["yarn.am.cores"].toString().toInt()
                sparkOpts.contains("driver.cores") -> sparkOpts["driver.cores"].toString().toInt()
                conf.yarn().worker().cores() > 0 -> conf.yarn().worker().cores()
                else -> 1
            }

            val mem: Int = when {
                sparkExecConfigurations.containsKey("spark.yarn.am.memory") -> MemoryFormatParser.extractMegabytes(sparkExecConfigurations["spark.yarn.am.memory"].toString())
                sparkExecConfigurations.containsKey("spark.driver.memeory") -> MemoryFormatParser.extractMegabytes(sparkExecConfigurations["spark.driver.memeory"].toString())
                sparkOpts.contains("yarn.am.memory") -> MemoryFormatParser.extractMegabytes(sparkOpts["yarn.am.memory"].get())
                sparkOpts.contains("driver.memory") -> MemoryFormatParser.extractMegabytes(sparkOpts["driver.memory"].get())
                conf.yarn().worker().memoryMB() > 0 -> conf.yarn().worker().memoryMB()
                conf.taskMem() > 0 -> conf.taskMem()
                else -> 1024
            }
            return DriverConfiguration(mem, cpu)
        }


}