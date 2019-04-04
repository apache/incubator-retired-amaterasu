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

import software.amazon.awssdk.services.s3.S3Client
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import org.apache.commons.validator.routines.UrlValidator
import org.jets3t.service.S3ServiceException
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider
import software.amazon.awssdk.core.sync.ResponseTransformer
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.s3.model.GetObjectRequest
import java.io.FileNotFoundException
import java.io.IOException
import java.lang.IllegalArgumentException
import java.net.URL
import java.nio.file.Paths

class FileUtil(accessKeyId: String = "", secretAccessKey: String = "") {

    private val schemes = arrayOf("http", "https", "s3", "s3a")
    private val urlValidator = UrlValidator(schemes)

    private var credentials: AwsCredentialsProvider = if (accessKeyId.isNotEmpty()) {
        StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKeyId, secretAccessKey))
    } else {
        InstanceProfileCredentialsProvider.builder().build()
    }

    fun downloadFile(remote: String, destination: String): Boolean {

        assert(isSupportedUrl(remote))
        val url = URL(remote)

        try {

            // https://s3-ap-southeast-2.amazonaws.com/amaterasu/BugBounty-TestUpload.txt
            val scheme = url.protocol //http
            if (scheme !in schemes) {
                throw IllegalArgumentException("${url.protocol} not supported")
            }

            val host = url.host // s3-ap-southeast-2.amazonaws.com
            val region: String = if (host == "s3.amazonaws.com") {
                "us-east-1" //N.Virginia
            } else {
                host.removePrefix("s3-").removeSuffix(".amazonaws.com")
            }

            val path = url.path.removePrefix("/") // /amaterasu/testfile.txt
            val split = path.split("/")
            val bucket = split[0]
            val key = split.subList(1, split.size).joinToString("/")

            val s3 = S3Client.builder()
                    .credentialsProvider(credentials)
                    .region(Region.of(region))
                    .build()

            val request = GetObjectRequest.builder()
                    .bucket(bucket)
                    .key(key)
                    .build()

            s3.getObject(request, ResponseTransformer.toFile(Paths.get(destination)))


            return true
        } catch (e: S3ServiceException) {
            System.err.println(e.message)
        } catch (e: FileNotFoundException) {
            System.err.println(e.message)
        } catch (e: IOException) {
            System.err.println(e.message)
        }
        return false
    }

    private fun isSupportedUrl(string: String): Boolean {
        return urlValidator.isValid(string)
    }

}