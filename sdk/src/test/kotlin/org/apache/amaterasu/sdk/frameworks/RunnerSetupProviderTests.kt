package org.apache.amaterasu.sdk.frameworks

import org.apache.amaterasu.common.configuration.enums.ActionStatus
import org.apache.amaterasu.common.dataobjects.ActionData
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.given
import org.jetbrains.spek.api.dsl.it
import kotlin.test.assertEquals

object RunnerSetupProviderTests : Spek({

    given("A class implementing RunnerSetupProvider") {

        val testProvider = TestRunnerProvider()
        val data = ActionData(ActionStatus.Started, "test", "test.scala", "spark", "scala-jar", "123")

        it("adds the default action resource files to the user action resource files") {
            val resources = testProvider.getActionResources("job", data)
            assertEquals(resources.size, 4)
        }
    }
})