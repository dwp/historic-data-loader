package app.load.services.impl

import app.load.domain.DataKeyResult
import app.load.exceptions.DataKeyDecryptionException
import app.load.exceptions.DataKeyServiceUnavailableException
import app.load.providers.HttpClientProvider
import app.load.utility.UUIDGenerator
import com.google.gson.Gson
import com.nhaarman.mockitokotlin2.*
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import org.apache.http.HttpEntity
import org.apache.http.StatusLine
import org.apache.http.client.methods.CloseableHttpResponse
import org.apache.http.client.methods.HttpGet
import org.apache.http.client.methods.HttpPost
import org.apache.http.client.methods.HttpUriRequest
import org.apache.http.impl.client.CloseableHttpClient
import java.io.ByteArrayInputStream

class HttpKeyServiceTest: StringSpec() {

    init {
        "testBatchDataKey_WillCallClientOnce_AndReturnKey" {
            val responseBody = """
                |{
                |    "dataKeyEncryptionKeyId": "DATAKEY_ENCRYPTION_KEY_ID",
                |    "plaintextDataKey": "PLAINTEXT_DATAKEY",
                |    "ciphertextDataKey": "CIPHERTEXT_DATAKEY"
                |}
            """.trimMargin()

            val byteArrayInputStream = ByteArrayInputStream(responseBody.toByteArray())
            val statusLine = mock<StatusLine>()
            val entity = mock<HttpEntity>()
            given(entity.content).willReturn(byteArrayInputStream)
            given(statusLine.statusCode).willReturn(201)
            val httpResponse = mock<CloseableHttpResponse>()
            given(httpResponse.statusLine).willReturn(statusLine)
            given(httpResponse.entity).willReturn(entity)
            val httpClient = mock<CloseableHttpClient>()
            given(httpClient.execute(any<HttpGet>())).willReturn(httpResponse)

            val httpClientProvider = mock<HttpClientProvider> {
                on { client() } doReturn httpClient
            }

            val uuidGenerator = mock<UUIDGenerator> {
                on { randomUUID() } doReturn "12345"
            }
            val keyService = HttpKeyService(httpClientProvider, uuidGenerator, "http://dks:8443")
            val dataKeyResult = keyService.batchDataKey()
            val expectedResult: DataKeyResult = Gson().fromJson(responseBody, DataKeyResult::class.java)
            dataKeyResult shouldBe expectedResult
            val argumentCaptor = argumentCaptor<HttpUriRequest>()
            verify(httpClient, times(1)).execute(argumentCaptor.capture())
            argumentCaptor.firstValue.uri.toString() shouldBe "http://dks:8443/datakey?correlationId=12345"
        }

        "testBatchDataKey_ServerError_ThrowsException" {
            val httpClient = mock<CloseableHttpClient>()
            val statusLine = mock<StatusLine>()
            //val entity = mock(HttpEntity::class.java)
            given(statusLine.statusCode).willReturn(503)
            val httpResponse = mock<CloseableHttpResponse>()
            given(httpResponse.statusLine).willReturn(statusLine)
            given(httpClient.execute(any<HttpGet>())).willReturn(httpResponse)
            val httpClientProvider = mock<HttpClientProvider> {
                on { client() } doReturn httpClient
            }

            val uuidGenerator = mock<UUIDGenerator> {
                on { randomUUID() } doReturn "12345"
            }

            shouldThrow<DataKeyServiceUnavailableException> {
                val keyService = HttpKeyService(httpClientProvider, uuidGenerator, "http://dks:8443")
                keyService.batchDataKey()
            }

            val argumentCaptor = argumentCaptor<HttpGet>()
            verify(httpClient, times(1)).execute(argumentCaptor.capture())
            argumentCaptor.firstValue.uri.toString() shouldBe "http://dks:8443/datakey?correlationId=12345"
        }

        // TODO: retries
        "testBatchDataKey_UnknownHttpError_ThrowsException_AndWillRetry" {
            val statusLine = mock<StatusLine>()
            given(statusLine.statusCode).willReturn(503)
            val httpResponse = mock<CloseableHttpResponse>()
            given(httpResponse.statusLine).willReturn(statusLine)
            val httpClient = mock<CloseableHttpClient>()
            given(httpClient.execute(any<HttpGet>())).willThrow(RuntimeException("Boom!"))
            val httpClientProvider = mock<HttpClientProvider> {
                on { client() } doReturn httpClient
            }
            val uuidGenerator = mock<UUIDGenerator> {
                on { randomUUID() } doReturn "12345"
            }

            shouldThrow<DataKeyServiceUnavailableException> {
                val keyService = HttpKeyService(httpClientProvider, uuidGenerator, "http://dks:8443")
                keyService.batchDataKey()
            }

            val argumentCaptor = argumentCaptor<HttpGet>()
            verify(httpClient, times(1)).execute(argumentCaptor.capture())
            argumentCaptor.firstValue.uri.toString() shouldBe "http://dks:8443/datakey?correlationId=12345"
        }

        // TODO: retries
        "testBatchDataKey_WhenErrorsOccur_WillRetryUntilSuccessful" {
            val responseBody = """
                |{
                |    "dataKeyEncryptionKeyId": "DATAKEY_ENCRYPTION_KEY_ID",
                |    "plaintextDataKey": "PLAINTEXT_DATAKEY",
                |    "ciphertextDataKey": "CIPHERTEXT_DATAKEY"
                |}
            """.trimMargin()

            val byteArrayInputStream = ByteArrayInputStream(responseBody.toByteArray())
            val statusLine = mock<StatusLine>()
            val entity = mock<HttpEntity>()
            given(entity.content).willReturn(byteArrayInputStream)
            given(statusLine.statusCode).willReturn(503, 503, 201)
            val httpResponse = mock<CloseableHttpResponse>()
            given(httpResponse.statusLine).willReturn(statusLine)
            given(httpResponse.entity).willReturn(entity)
            val httpClient = mock<CloseableHttpClient>()
            given(httpClient.execute(any<HttpGet>())).willReturn(httpResponse)
            val httpClientProvider = mock<HttpClientProvider> {
                on { client() } doReturn httpClient
            }
            val uuidGenerator = mock<UUIDGenerator> {
                on { randomUUID() } doReturn "12345"
            }

            val keyService = HttpKeyService(httpClientProvider, uuidGenerator, "http://dks:8443")


            // TODO retries
            shouldThrow<DataKeyServiceUnavailableException> {
                keyService.batchDataKey()
            }

//            val expectedResult: DataKeyResult = Gson().fromJson(responseBody, DataKeyResult::class.java)
//            dataKeyResult shouldBe expectedResult

            val argumentCaptor = argumentCaptor<HttpGet>()
            verify(httpClient, times(1)).execute(argumentCaptor.capture())
            argumentCaptor.firstValue.uri.toString() shouldBe "http://dks:8443/datakey?correlationId=12345"
        }

        "testDecryptKey_HappyCase_CallsServerOnce_AndReturnsUnencryptedData" {
            val responseBody = """
                |{
                |  "dataKeyEncryptionKeyId": "DATAKEY_ENCRYPTION_KEY_ID",
                |  "plaintextDataKey": "PLAINTEXT_DATAKEY"
                |}
            """.trimMargin()

            val byteArrayInputStream = ByteArrayInputStream(responseBody.toByteArray())
            val statusLine = mock<StatusLine>()
            val entity = mock<HttpEntity>()
            given(entity.content).willReturn(byteArrayInputStream)
            given(statusLine.statusCode).willReturn(200)
            val httpResponse = mock<CloseableHttpResponse>()
            given(httpResponse.statusLine).willReturn(statusLine)
            given(httpResponse.entity).willReturn(entity)
            val httpClient = mock<CloseableHttpClient>()
            given(httpClient.execute(any<HttpPost>())).willReturn(httpResponse)
            val httpClientProvider = mock<HttpClientProvider> {
                on { client() } doReturn httpClient
            }
            val uuidGenerator = mock<UUIDGenerator> {
                on { randomUUID() } doReturn "12345"
            }
            val keyService = HttpKeyService(httpClientProvider, uuidGenerator, "http://dks:8443")
            val dataKeyResult = keyService.decryptKey("123", "ENCRYPTED_KEY_ID")

            dataKeyResult shouldBe "PLAINTEXT_DATAKEY"
            val argumentCaptor = argumentCaptor<HttpPost>()
            verify(httpClient, times(1)).execute(argumentCaptor.capture())
            argumentCaptor.firstValue.uri.toString() shouldBe "http://dks:8443/datakey/actions/decrypt?keyId=123&correlationId=12345"
        }

        "testDecryptKey_HappyCase_WillCallServerOnce_AndCacheResponse" {
            val responseBody = """
                |{
                |  "dataKeyEncryptionKeyId": "DATAKEY_ENCRYPTION_KEY_ID",
                |  "plaintextDataKey": "PLAINTEXT_DATAKEY"
                |}
            """.trimMargin()

            val byteArrayInputStream = ByteArrayInputStream(responseBody.toByteArray())
            val statusLine = mock<StatusLine>()
            val entity = mock<HttpEntity>()
            given(entity.content).willReturn(byteArrayInputStream)
            given(statusLine.statusCode).willReturn(200)
            val httpResponse = mock<CloseableHttpResponse>()
            given(httpResponse.statusLine).willReturn(statusLine)
            given(httpResponse.entity).willReturn(entity)
            val httpClient = mock<CloseableHttpClient>()
            given(httpClient.execute(any<HttpPost>())).willReturn(httpResponse)
            val httpClientProvider = mock<HttpClientProvider> {
                on { client() } doReturn httpClient
            }
            val uuidGenerator = mock<UUIDGenerator> {
                on { randomUUID() } doReturn "12345"
            }
            val keyService = HttpKeyService(httpClientProvider, uuidGenerator, "http://dks:8443")

            val dataKeyResult = keyService.decryptKey("123", "ENCRYPTED_KEY_ID")
            dataKeyResult shouldBe "PLAINTEXT_DATAKEY"

            val argumentCaptor = argumentCaptor<HttpPost>()
            verify(httpClient, times(1)).execute(argumentCaptor.capture())
            argumentCaptor.firstValue.uri.toString() shouldBe "http://dks:8443/datakey/actions/decrypt?keyId=123&correlationId=12345"
        }

        "testDecryptKey_WithABadKey_WillCallServerOnce_AndNotRetry" {
            val statusLine = mock<StatusLine>()
            given(statusLine.statusCode).willReturn(400)
            val httpResponse = mock<CloseableHttpResponse>()
            given(httpResponse.statusLine).willReturn(statusLine)
            val httpClient = mock<CloseableHttpClient>()
            given(httpClient.execute(any<HttpPost>())).willReturn(httpResponse)
            val httpClientProvider = mock<HttpClientProvider> {
                on { client() } doReturn httpClient
            }
            val uuidGenerator = mock<UUIDGenerator> {
                on { randomUUID() } doReturn "12345"
            }
            val keyService = HttpKeyService(httpClientProvider, uuidGenerator, "http://dks:8443")

            val ex = shouldThrow<DataKeyDecryptionException> {
                keyService.decryptKey("123", "ENCRYPTED_KEY_ID")
            }
            ex.message shouldBe "Decrypting encryptedKey: 'ENCRYPTED_KEY_ID' with keyEncryptionKeyId: '123', dks_correlation_id: '12345' data key service returned status code '400'"
            val argumentCaptor = argumentCaptor<HttpPost>()
            verify(httpClient, times(1)).execute(argumentCaptor.capture())
            argumentCaptor.firstValue.uri.toString() shouldBe "http://dks:8443/datakey/actions/decrypt?keyId=123&correlationId=12345"
        }

    }
}

//
//        @Test
//        fun testDecryptKey_ServerError_WillCauseRetryMaxTimes() {
//            val statusLine = mock(StatusLine::class.java)
//            given(statusLine.statusCode).willReturn(503)
//            val httpResponse = mock(CloseableHttpResponse::class.java)
//            given(httpResponse.statusLine).willReturn(statusLine)
//            val httpClient = mock(CloseableHttpClient::class.java)
//            given(httpClient.execute(any(HttpPost::class.java))).willReturn(httpResponse)
//            given(httpClientProvider.client()).willReturn(httpClient)
//            val dksCallId = nextDksCorrelationId()
//            whenever(uuidGenerator.randomUUID()).thenReturn(dksCallId)
//
//            try {
//                keyService.decryptKey("123", "ENCRYPTED_KEY_ID")
//                fail("Should throw a DataKeyServiceUnavailableException")
//            }
//            catch (ex: DataKeyServiceUnavailableException) {
//                assertEquals("Decrypting encryptedKey: 'ENCRYPTED_KEY_ID' with keyEncryptionKeyId: '123', dks_correlation_id: '$dksCallId' data key service returned status code '503'", ex.message)
//                val argumentCaptor = ArgumentCaptor.forClass(HttpPost::class.java)
//                verify(httpClient, times(5)).execute(argumentCaptor.capture())
//                assertEquals("http://dummydks/datakey/actions/decrypt?keyId=123&correlationId=$dksCallId", argumentCaptor.firstValue.uri.toString())
//            }
//        }
//
//        @Test
//        fun testDecryptKey_UnknownHttpError_WillCauseRetryMaxTimes() {
//            val statusLine = mock(StatusLine::class.java)
//            given(statusLine.statusCode).willReturn(503)
//            val httpResponse = mock(CloseableHttpResponse::class.java)
//            given(httpResponse.statusLine).willReturn(statusLine)
//            val httpClient = mock(CloseableHttpClient::class.java)
//            given(httpClient.execute(any(HttpPost::class.java))).willThrow(RuntimeException("Boom!"))
//            given(httpClientProvider.client()).willReturn(httpClient)
//            val dksCallId = nextDksCorrelationId()
//            whenever(uuidGenerator.randomUUID()).thenReturn(dksCallId)
//
//            try {
//                keyService.decryptKey("123", "ENCRYPTED_KEY_ID")
//                fail("Should throw a DataKeyServiceUnavailableException")
//            }
//            catch (ex: DataKeyServiceUnavailableException) {
//                assertEquals("Error contacting data key service: 'java.lang.RuntimeException: Boom!', dks_correlation_id: '$dksCallId'", ex.message)
//                val argumentCaptor = ArgumentCaptor.forClass(HttpPost::class.java)
//                verify(httpClient, times(5)).execute(argumentCaptor.capture())
//                assertEquals("http://dummydks/datakey/actions/decrypt?keyId=123&correlationId=$dksCallId", argumentCaptor.firstValue.uri.toString())
//            }
//        }
//
//        @Test
//        fun testDecryptKey_WhenErrorOccur_WillRetryUntilSuccessful() {
//            val responseBody = """
//            |{
//            |  "dataKeyEncryptionKeyId": "DATAKEY_ENCRYPTION_KEY_ID",
//            |  "plaintextDataKey": "PLAINTEXT_DATAKEY"
//            |}
//        """.trimMargin()
//
//            val byteArrayInputStream = ByteArrayInputStream(responseBody.toByteArray())
//            val mockStatusLine = mock(StatusLine::class.java)
//            val entity = mock(HttpEntity::class.java)
//            given(entity.content).willReturn(byteArrayInputStream)
//            given(mockStatusLine.statusCode).willReturn(503, 503, 200)
//            val httpResponse = mock(CloseableHttpResponse::class.java)
//            given(httpResponse.statusLine).willReturn(mockStatusLine)
//            given(httpResponse.entity).willReturn(entity)
//            val httpClient = mock(CloseableHttpClient::class.java)
//            given(httpClient.execute(any(HttpPost::class.java))).willReturn(httpResponse)
//            given(httpClientProvider.client()).willReturn(httpClient)
//            val dksCallId = nextDksCorrelationId()
//            whenever(uuidGenerator.randomUUID()).thenReturn(dksCallId)
//
//            val dataKeyResult = keyService.decryptKey("123", "ENCRYPTED_KEY_ID")
//
//            assertEquals("PLAINTEXT_DATAKEY", dataKeyResult)
//            val argumentCaptor = ArgumentCaptor.forClass(HttpPost::class.java)
//            verify(httpClient, times(3)).execute(argumentCaptor.capture())
//            assertEquals("http://dummydks/datakey/actions/decrypt?keyId=123&correlationId=$dksCallId", argumentCaptor.firstValue.uri.toString())
//        }
//
//
