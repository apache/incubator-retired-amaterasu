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
package org.apache.amaterasu.frameworks.jvm.common.configuration.dataset

import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.given
import org.jetbrains.spek.api.dsl.it
import java.io.File
import kotlin.test.assertEquals
import kotlin.test.assertTrue

object DataSetConfigManagerEncryptionTests : Spek({

    val marker = DataSetConfigManagerEncryptionTests::class.java.getResource("/maki.yml")!!.path

    given("a ConfigManager for configuration for datasets where there is a key element") {
        val repoPath = "${File(marker).parent}/encryption_test_repo"
        val cfg = DataSetConfigManager("test", repoPath)

        it("should load the key configuration for files config") {
            val fileConfigs = cfg.getFileConfigs()
            assertEquals(1, fileConfigs.size)
            assertTrue { fileConfigs[0].key.isInitialized }
        }
    }
})