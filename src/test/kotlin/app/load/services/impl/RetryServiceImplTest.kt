package app.load.services.impl

import app.load.domain.DataKeyResult
import app.load.exceptions.DataKeyServiceUnavailableException
import app.load.services.KeyService
import com.nhaarman.mockitokotlin2.*
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe

class RetryServiceImplTest: StringSpec() {
    init {

        "decryptDataKey() does not retry on success" {
            val keyService = mock<KeyService> {
                on { decryptKey("encryptionKeyId", "encryptedKey") } doReturn "plaintextKey"
            }

            val retryUtility = RetryServiceImpl(keyService, 5, 1, 1)
            val actual = retryUtility.decryptKey("encryptionKeyId", "encryptedKey")
            actual shouldBe "plaintextKey"
            verify(keyService, times(1)).decryptKey("encryptionKeyId", "encryptedKey")
            verifyNoMoreInteractions(keyService)
        }

        "decryptDataKey() does retry on failure" {
            val keyService = mock<KeyService> {
                on {
                    decryptKey("encryptionKeyId", "encryptedKey")
                } doThrow DataKeyServiceUnavailableException("Failure1") doThrow DataKeyServiceUnavailableException("Failure2") doReturn "plaintextKey"
            }
            val retryUtility = RetryServiceImpl(keyService, 5, 1, 1)
            val actual = retryUtility.decryptKey("encryptionKeyId", "encryptedKey")
            actual shouldBe "plaintextKey"
            verify(keyService, times(3)).decryptKey("encryptionKeyId", "encryptedKey")
            verifyNoMoreInteractions(keyService)
        }

        "decryptKey() gives up after max retries" {
            val keyService = mock<KeyService> {
                on {
                    decryptKey("encryptionKeyId", "encryptedKey")
                } doThrow DataKeyServiceUnavailableException("Failure")
            }

            val retryUtility = RetryServiceImpl(keyService, 5, 1, 1)

            val e = shouldThrow<DataKeyServiceUnavailableException> {
                retryUtility.decryptKey("encryptionKeyId", "encryptedKey")
            }

            verify(keyService, times(5)).decryptKey("encryptionKeyId", "encryptedKey")
            verifyNoMoreInteractions(keyService)
            e.message shouldBe "Failure"
        }

        "batchDataKey() does not retry on success" {
            val dataKeyResult = dataKeyResult()
            val keyService = mock<KeyService> {
                on { batchDataKey() } doReturn dataKeyResult
            }

            val retryUtility = RetryServiceImpl(keyService, 5, 1, 1)

            val actual = retryUtility.batchDataKey()

            actual shouldBe dataKeyResult
            verify(keyService, times(1)).batchDataKey()
            verifyNoMoreInteractions(keyService)
        }

        "batchDataKey() does retry on failure" {
            val dataKeyResult = dataKeyResult()
            val keyService = mock<KeyService> {
                on {
                    batchDataKey()
                } doThrow DataKeyServiceUnavailableException("Failure1") doThrow DataKeyServiceUnavailableException("Failure2") doReturn dataKeyResult
            }

            val retryUtility = RetryServiceImpl(keyService, 5, 1, 1)

            val actual = retryUtility.batchDataKey()

            actual shouldBe dataKeyResult
            verify(keyService, times(3)).batchDataKey()
            verifyNoMoreInteractions(keyService)
        }

        "batchDataKey() gives up after max retries" {
            val keyService = mock<KeyService> {
                on {
                    batchDataKey()
                } doThrow DataKeyServiceUnavailableException("Failure")
            }

            val retryUtility = RetryServiceImpl(keyService, 5, 1, 1)

            val e = shouldThrow<DataKeyServiceUnavailableException> {
                retryUtility.batchDataKey()
            }

            verify(keyService, times(5)).batchDataKey()
            verifyNoMoreInteractions(keyService)
            e.message shouldBe "Failure"
        }

    }

    private fun dataKeyResult(): DataKeyResult =
            DataKeyResult("dataKeyEncryptionKeyId","plaintextDataKey", "ciphertextDataKey")
}
