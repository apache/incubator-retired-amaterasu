/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.amaterasu.common.utils

import org.apache.commons.validator.routines.UrlValidator

object FileUtil {

    private val schemes = arrayOf("http", "https", "s3", "s3a")
    private val urlValidator = UrlValidator(schemes)

    fun downloadFile(remote: String) {

    }

    fun isSupportedUrl(string: String) {
        urlValidator.isValid(string)
    }

}