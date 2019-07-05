package org.apache.amaterasu.common.utils

import org.apache.amaterasu.common.crypto.AwsKmsCryptoProvider
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.given
import org.jetbrains.spek.api.dsl.it
import org.jetbrains.spek.api.dsl.on
import org.junit.Assert.*


//class CryptoUtilsTest : Spek({
//
//    given("an aws key and value") {
//        val keyId = "arn:aws:kms:"
//        val clearText = "Value to encrypt"
//
//        on ("encrypting and decrypting") {
//
//            val kmsClient = CryptoUtils(AwsKmsCryptoProvider(
//                    accessKeyId = "",
//                    secretAccessKey = "",
//                    region = "ap-southeast-2"))
//            val encryptedText = kmsClient.encryptKey(keyId, clearText)!!
//
//            val decryptedText = kmsClient.decryptKey(encryptedText)
//
//            it("decrypted value is the same as the clear text") {
//                assertEquals(clearText, decryptedText)
//            }
//        }
//    }
//
//})