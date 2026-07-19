package com.arnia.gateway.crypto

import org.apache.commons.codec.binary.Base64
import org.slf4j.LoggerFactory
import javax.crypto.Cipher
import javax.crypto.SecretKey
import javax.crypto.SecretKeyFactory
import javax.crypto.spec.DESedeKeySpec

/**
 * Triple-DES (DESede) encrypt/decrypt — matches the implementation in baltazar-server.
 * Keys are Base64-encoded 24-byte arrays stored in GameRunEntity.encryptionKey.
 */
class TripleDes(
    keyBytes: ByteArray,
) {
    private val key: SecretKey
    private val cipher: Cipher

    init {
        val ks = DESedeKeySpec(keyBytes)
        val skf = SecretKeyFactory.getInstance(SCHEME)
        cipher = Cipher.getInstance(SCHEME)
        key = skf.generateSecret(ks)
    }

    @Synchronized
    fun encrypt(plainText: String): String? =
        runCatching {
            cipher.init(Cipher.ENCRYPT_MODE, key)
            val encrypted = cipher.doFinal(plainText.toByteArray(Charsets.UTF_8))
            String(Base64.encodeBase64(encrypted))
        }.getOrElse {
            log.error("TripleDes encrypt error", it)
            null
        }

    @Synchronized
    fun decrypt(encryptedText: String): String? =
        runCatching {
            cipher.init(Cipher.DECRYPT_MODE, key)
            val decrypted = cipher.doFinal(Base64.decodeBase64(encryptedText))
            String(decrypted, Charsets.UTF_8)
        }.getOrElse {
            log.error("TripleDes decrypt error", it)
            null
        }

    companion object {
        private const val SCHEME = "DESede"
        private val log = LoggerFactory.getLogger(TripleDes::class.java)

        fun fromBase64Key(base64Key: String) = TripleDes(Base64.decodeBase64(base64Key))
    }
}
