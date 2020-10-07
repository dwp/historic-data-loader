package app.load.mapreduce

import app.load.domain.DataKeyResult
import app.load.exceptions.DataKeyServiceUnavailableException
import app.load.services.KeyService
import com.nhaarman.mockitokotlin2.*
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe

class RetryUtilityImplTest: StringSpec() {
    init {

        "batchDataKey() does not retry on success" {
            val dataKeyResult = dataKeyResult()
            val keyService = mock<KeyService> {
                on { batchDataKey() } doReturn dataKeyResult
            }

            val retryUtility = RetryUtilityImpl(keyService, 5, 1, 1)

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

            val retryUtility = RetryUtilityImpl(keyService, 5, 1, 1)

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

            val retryUtility = RetryUtilityImpl(keyService, 5, 1, 1)

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
