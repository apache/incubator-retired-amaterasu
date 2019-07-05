package org.apache.amaterasu.common.crypto

import com.amazonaws.services.identitymanagement.model.AccessKey
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider
import software.amazon.awssdk.auth.credentials.InstanceProfileCredentialsProvider
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.core.SdkBytes
import software.amazon.awssdk.services.kms.KmsClient
import software.amazon.awssdk.services.kms.model.DecryptRequest
import software.amazon.awssdk.services.kms.model.EncryptRequest
import software.amazon.awssdk.regions.Region

class AwsKmsCryptoProvider(override val accessKeyId: String,
                           override val secretAccessKey: String,
                           override val region: String) : CryptoKeyProvider {

    private var credentials: AwsCredentialsProvider = if (accessKeyId.isNotEmpty()) {
        StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKeyId, secretAccessKey))
    } else {
        InstanceProfileCredentialsProvider.builder().build()
    }

    val kmsClient = KmsClient.builder().apply {
        credentialsProvider(credentials)
        region(Region.of(region))
    }.build()

    override fun decryptKey(cipherText: ByteArray): String {

        val decryptRequest = DecryptRequest.builder().apply {
            ciphertextBlob(SdkBytes.fromByteArray(cipherText))
        }.build()

        val decryptedValue = kmsClient.decrypt(decryptRequest).plaintext()

        return decryptedValue.asUtf8String()

    }

    override fun encryptKey(keyId: String, value: String): ByteArray? {

        val encryptRequest = EncryptRequest.builder().apply {
            keyId(keyId)
            plaintext(SdkBytes.fromUtf8String(value))
        }.build()

        val encryptResponse = kmsClient.encrypt(encryptRequest)

        return encryptResponse.ciphertextBlob().asByteArray()
    }

}