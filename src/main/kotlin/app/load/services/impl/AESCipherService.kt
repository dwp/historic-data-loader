package app.load.services.impl

import app.configuration.CipherInstanceProvider
import app.load.domain.EncryptionResult
import app.load.services.CipherService
import org.apache.commons.compress.compressors.CompressorStreamFactory
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream
import org.bouncycastle.jce.provider.BouncyCastleProvider
import uk.gov.dwp.dataworks.logging.DataworksLogger
import java.io.InputStream
import java.security.Key
import java.security.SecureRandom
import java.security.Security
import java.util.*
import javax.crypto.Cipher
import javax.crypto.CipherInputStream
import javax.crypto.spec.IvParameterSpec
import javax.crypto.spec.SecretKeySpec

class AESCipherService(private val secureRandom: SecureRandom, private val cipherInstanceProvider: CipherInstanceProvider) : CipherService {

    init {
        Security.addProvider(BouncyCastleProvider())
    }

    override fun decompressingDecryptingStream(inputStream: InputStream, key: Key, iv: String) =
        CompressorStreamFactory().createCompressorInputStream(CompressorStreamFactory.GZIP,
            decryptingInputStream(inputStream, key, iv)) as GzipCompressorInputStream

    private fun decryptingInputStream(inputStream: InputStream, key: Key, iv: String) =
        CipherInputStream(inputStream, decryptingCipher(key, iv))


    private fun decryptingCipher(key: Key, iv: String) =
        cipherInstanceProvider.cipherInstance().apply {
            init(Cipher.DECRYPT_MODE, key, IvParameterSpec(Base64.getDecoder().decode(iv)))
        }

    override fun encrypt(key: String, unencrypted: ByteArray): EncryptionResult {
        val initialisationVector = ByteArray(16).apply {
            secureRandom.nextBytes(this)
        }

        val keySpec: Key = SecretKeySpec(Base64.getDecoder().decode(key), "AES")
        val cipher = Cipher.getInstance(targetCipherAlgorithm, "BC").apply {
            init(Cipher.ENCRYPT_MODE, keySpec, IvParameterSpec(initialisationVector))
        }

        val encrypted = cipher.doFinal(unencrypted)
        return EncryptionResult(String(Base64.getEncoder().encode(initialisationVector)),
            String(Base64.getEncoder().encode(encrypted)))
    }

    private val targetCipherAlgorithm: String = "AES/CTR/NoPadding"

    companion object {
        val logger = DataworksLogger.getLogger(AESCipherService::class.java.toString())
    }
}
