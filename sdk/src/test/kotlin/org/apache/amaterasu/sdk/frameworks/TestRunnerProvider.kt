package org.apache.amaterasu.sdk.frameworks

import org.apache.amaterasu.common.dataobjects.ActionData

class TestRunnerProvider() : RunnerSetupProvider() {
    override val hasExecutor: Boolean
        get() = false

    override val runnerResources: Array<String>
        get() = arrayOf()

    override fun getCommand(jobId: String, actionData: ActionData, env: String, executorId: String, callbackAddress: String): String {
        return ""
    }

    override fun getActionUserResources(jobId: String, actionData: ActionData): Array<String> {
        return arrayOf("testresource.yaml")
    }

    override fun getActionDependencies(jobId: String, actionData: ActionData): Array<String> {
        return arrayOf()
    }
}