///*
// * Licensed to the Apache Software Foundation (ASF) under one or more
// * contributor license agreements.  See the NOTICE file distributed with
// * this work for additional information regarding copyright ownership.
// * The ASF licenses this file to You under the Apache License, Version 2.0
// * (the "License"); you may not use this file except in compliance with
// * the License.  You may obtain a copy of the License at
// *
// *      http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//package org.apache.amaterasu.frameworks.python.dispatcher
//import org.apache.amaterasu.common.configuration.ClusterConfig
//import org.apache.amaterasu.common.configuration.enums.ActionStatus
//import org.apache.amaterasu.common.dataobjects.ActionData
//import org.apache.amaterasu.frameworks.python.dispatcher.runners.providers.BasicPythonRunnerProvider
//import org.jetbrains.spek.api.Spek
//import org.jetbrains.spek.api.dsl.given
//import org.jetbrains.spek.api.dsl.it
//import org.jetbrains.spek.api.dsl.on
//import kotlin.test.assertEquals
//import kotlin.test.assertNotEquals
//import kotlin.test.assertNotNull
//import java.io.File
//
//
//class BasicPythonRunnerProviderTests: Spek({
//
//    given("A python runner provider") {
//        val resourceRepoUri = this.javaClass.getResource("/test/repo")
//        val resourceRepo = File(resourceRepoUri.file)
//        val testRepo = File("repo")
//        testRepo.deleteRecursively()
//        if (File("requirements.txt").exists())
//            File("requirements.txt").delete()
//        resourceRepo.copyRecursively(testRepo)
//        val runner = BasicPythonRunnerProvider("test", ClusterConfig())
//        on("Asking to run a simple python script with dummy actionData") {
//            val command = runner.getCommand("AAAA",
//                    ActionData(ActionStatus.Pending,
//                            "AAA",
//                            "AAA",
//                            "AAA",
//                            "AAA",
//                            "AAA",
//                            "",
//                            emptyMap()),
//                    "",
//                    "",
//                    "")
//            it("should yield a command") {
//                assertNotNull(command)
//            }
//            it("should yield a non empty command") {
//                assertNotEquals("", command)
//            }
//
//        }
//        on("asking to run a simple python script with dependencies") {
//            val actionData = ActionData(
//                    ActionStatus.Queued,
//                    "simple",
//                    "simple.py",
//                    "python",
//                    "python",
//                    "Test",
//                    "",
//                    emptyMap())
//            val command = runner.getCommand("Test", actionData, "", "", "")
//            runner.getActionDependencies("Test", actionData)
//            it("Should yield command that runs simple.py") {
//                assertEquals("pip install -r ama-requirements.txt && pip install -r requirements.txt && python simple.py", command)
//            }
//            it("Should create a requirements file with all the dependencies in it") {
//                val requirements = File("ama-requirements.txt").readLines().toTypedArray()
//                assertEquals(arrayOf("./python_sdk.zip").joinToString(","), requirements.joinToString(","))
//            }
//        }
//    }
//    afterGroup {
//        File("ama-requirements.txt").delete()
//    }
//})