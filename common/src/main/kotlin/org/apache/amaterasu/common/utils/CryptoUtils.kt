package org.apache.amaterasu.common.utils

import org.apache.amaterasu.common.crypto.CryptoKeyProvider

class CryptoUtils(private val keyProvider: CryptoKeyProvider) {

    fun decryptKey(encryptedKey: ByteArray): String {
        return keyProvider.decryptKey(encryptedKey)
    }

    fun encryptKey(keyId: String, value: String): ByteArray? {
        return keyProvider.encryptKey(keyId, value)
    }

}