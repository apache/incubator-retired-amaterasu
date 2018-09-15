package org.apache.amaterasu.frameworks.python.dispatcher.runners.providers

import org.apache.amaterasu.common.configuration.ClusterConfig
import org.apache.amaterasu.common.dataobjects.ActionData
import org.apache.amaterasu.common.dataobjects.ExecData
import org.apache.amaterasu.leader.common.utilities.DataLoader
import org.apache.amaterasu.sdk.frameworks.RunnerSetupProvider
import java.io.File

abstract class PythonRunnerProviderBase(env: String?, conf:ClusterConfig?) : RunnerSetupProvider {

    val execData: ExecData = DataLoader.getExecutorData(env, conf)

    override fun getCommand(jobId: String?, actionData: ActionData?, env: String?, executorId: String?, callbackAddress: String?): String {
        return "pip install -r {deps}"
    }

    override fun getRunnerResources(): Array<String> {
        return arrayOf("python_sdk.zip")
    }

    override fun getActionDependencies(jobId: String?, actionData: ActionData?): Array<String> {
        File("requirements.txt").appendText("./python_sdk.zip\n")
        for (dep in execData.pyDeps().packages())
            if (dep.index().isDefined && !dep.index().isEmpty)
                File("requirements.txt").appendText("-i ${dep.index()} ${dep.packageId()}")
            else
                File("requirements.txt").appendText(dep.packageId())
        return arrayOf("requirements.txt")
    }
}