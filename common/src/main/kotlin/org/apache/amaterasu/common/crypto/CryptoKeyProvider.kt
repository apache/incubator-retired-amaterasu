package org.apache.amaterasu.common.crypto

interface CryptoKeyProvider {

    val accessKeyId: String
    val secretAccessKey: String
    val region: String

    fun decryptKey(cipherText: ByteArray): String

    fun encryptKey(keyId: String, value: String): ByteArray?

}