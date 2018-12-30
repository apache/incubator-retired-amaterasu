package org.apache.amaterasu.frameworks.python.dispatcher
import org.apache.amaterasu.common.configuration.ClusterConfig
import org.apache.amaterasu.common.configuration.enums.ActionStatus
import org.apache.amaterasu.common.dataobjects.ActionData
import org.apache.amaterasu.frameworks.python.dispatcher.runners.providers.BasicPythonRunnerProvider
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.given
import org.jetbrains.spek.api.dsl.it
import org.jetbrains.spek.api.dsl.on
import kotlin.test.assertEquals
import kotlin.test.assertNotEquals
import kotlin.test.assertNotNull
import java.io.File


class BasicPythonRunnerProviderTests: Spek({

    given("A python runner provider") {
        val resourceRepoUri = this.javaClass.getResource("/test/repo")
        val resourceRepo = File(resourceRepoUri.file)
        val testRepo = File("repo")
        testRepo.deleteRecursively()
        if (File("requirements.txt").exists())
            File("requirements.txt").delete()
        resourceRepo.copyRecursively(testRepo)
        val runner = BasicPythonRunnerProvider("test", ClusterConfig())
        on("Asking to run a simple python script with dummy actionData") {
            val command = runner.getCommand("AAAA",
                    ActionData(ActionStatus.pending,
                            "AAA",
                            "AAA",
                            "AAA",
                            "AAA",
                            "AAA",
                            "",
                            emptyMap()),
                    "",
                    "",
                    "")
            it("should yield a command") {
                assertNotNull(command)
            }
            it("should yield a non empty command") {
                assertNotEquals("", command)
            }

        }
        on("asking to run a simple python script with dependencies") {
            val actionData = ActionData(
                    ActionStatus.queued,
                    "simple",
                    "simple.py",
                    "python",
                    "python",
                    "Test",
                    "",
                    emptyMap())
            val command = runner.getCommand("Test", actionData, "", "", "")
            runner.getActionDependencies("Test", actionData)
            it("Should yield command that runs simple.py") {
                assertEquals("pip install -r requirements.txt && python simple.py", command)
            }
            it("Should create a requirements file with all the dependencies in it") {
                val requirements = File("requirements.txt").readLines().toTypedArray()
                assertEquals(arrayOf("./python_sdk.zip", "pandas", "twython").joinToString(","), requirements.joinToString(","))
            }
        }
    }


})