package org.apache.amaterasu.frameworks.python.dispatcher.runners.providers

import org.apache.amaterasu.common.configuration.ClusterConfig
import org.apache.amaterasu.common.dataobjects.ActionData

open class BasicPythonRunnerProvider(env: String?, conf: ClusterConfig?): PythonRunnerProviderBase(env, conf) {
    override fun getActionResources(jobId: String?, actionData: ActionData?): Array<String> {
        return Array(0) { _ -> ""}
    }

    override fun getCommand(jobId: String?, actionData: ActionData?, env: String?, executorId: String?, callbackAddress: String?): String {
        return super.getCommand(jobId, actionData, env, executorId, callbackAddress) + " && python ${actionData!!.src()}"
    }

    override fun getRunnerResources(): Array<String> {
        var resources = super.getRunnerResources()
        resources = resources.copyOf(resources.size + 1).requireNoNulls()
        resources[resources.size] = "runtime.zip"
        return resources
    }

}