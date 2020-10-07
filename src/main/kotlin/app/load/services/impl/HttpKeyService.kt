package app.load.services.impl

import app.load.configurations.HttpClientProviderConfiguration
import app.load.domain.DataKeyResult
import app.load.exceptions.DataKeyDecryptionException
import app.load.exceptions.DataKeyServiceUnavailableException
import app.load.providers.HttpClientProvider
import app.load.providers.SecureHttpClientProvider
import app.load.services.KeyService
import app.load.utility.UUIDGenerator
import app.load.utility.impl.UUIDGeneratorImpl
import com.google.gson.Gson
import org.apache.http.client.methods.HttpGet
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.ContentType
import org.apache.http.entity.StringEntity
import org.apache.http.util.EntityUtils
import uk.gov.dwp.dataworks.logging.DataworksLogger
import java.io.BufferedReader
import java.io.InputStreamReader
import java.io.Reader
import java.net.URLEncoder

class HttpKeyService(private val httpClientProvider: HttpClientProvider,
                     private val uuidGenerator: UUIDGenerator,
                     private val dataKeyServiceUrl: String): KeyService {

    override fun decryptKey(encryptionKeyId: String, encryptedKey: String): String {
        val dksCorrelationId = uuidGenerator.randomUUID()
        logger.info("Calling decryptKey: encryptedKey:'$encryptedKey', keyEncryptionKeyId: '$encryptionKeyId', dks_correlation_id: '$dksCorrelationId'")
        try {
            val cacheKey = "$encryptedKey/$encryptionKeyId"
            return if (decryptedKeyCache.containsKey(cacheKey)) {
                decryptedKeyCache[cacheKey]!!
            }
            else {
                httpClientProvider.client().use { client ->
                    val dksUrl = "$dataKeyServiceUrl/datakey/actions/decrypt?keyId=${URLEncoder.encode(encryptionKeyId, "US-ASCII")}"
                    val dksUrlWithCorrelationId = "$dksUrl&correlationId=$dksCorrelationId"
                    logger.info("Calling decryptKey: dks_url: '$dksUrl', dks_correlation_id: '$dksCorrelationId'")
                    val httpPost = HttpPost(dksUrlWithCorrelationId)
                    httpPost.entity = StringEntity(encryptedKey, ContentType.TEXT_PLAIN)
                    client.execute(httpPost).use { response ->
                        val statusCode = response.statusLine.statusCode
                        logger.info("Calling decryptKey: dks_url: '$dksUrl', dks_correlation_id: '$dksCorrelationId' returned status_code: '$statusCode'.")
                        return when (statusCode) {
                            200 -> {
                                val entity = response.entity
                                val text = BufferedReader(InputStreamReader(response.entity.content) as Reader).use(BufferedReader::readText)
                                EntityUtils.consume(entity)
                                val dataKeyResult = Gson().fromJson(text, DataKeyResult::class.java)
                                decryptedKeyCache[cacheKey] = dataKeyResult.plaintextDataKey
                                dataKeyResult.plaintextDataKey
                            }
                            400 ->
                                throw DataKeyDecryptionException(
                                    "Decrypting encryptedKey: '$encryptedKey' with keyEncryptionKeyId: '$encryptionKeyId', dks_correlation_id: '$dksCorrelationId' data key service returned status code '$statusCode'")
                            else ->
                                throw DataKeyServiceUnavailableException(
                                    "Decrypting encryptedKey: '$encryptedKey' with keyEncryptionKeyId: '$encryptionKeyId', dks_correlation_id: '$dksCorrelationId' data key service returned status code '$statusCode'")
                        }
                    }
                }
            }
        }
        catch (ex: Exception) {
            when (ex) {
                is DataKeyDecryptionException, is DataKeyServiceUnavailableException -> {
                    throw ex
                }
                else -> throw DataKeyServiceUnavailableException("Error contacting data key service: '$ex', dks_correlation_id: '$dksCorrelationId'")
            }
        }
    }

    override fun batchDataKey(): DataKeyResult {
        val dksCorrelationId = uuidGenerator.randomUUID()
        val dksUrl = "$dataKeyServiceUrl/datakey"
        val dksUrlWithCorrelationId = "$dksUrl?correlationId=$dksCorrelationId"
        try {
            logger.info("Calling batchDataKey", "dks_url" to dksUrl, "dks_correlation_id" to dksCorrelationId)
            httpClientProvider.client().use { client ->
                client.execute(HttpGet(dksUrlWithCorrelationId)).use { response ->
                    val statusCode = response.statusLine.statusCode
                    logger.info("Called batchDataKey", "dks_url" to dksUrl, "dks_correlation_id" to dksCorrelationId, "status_code" to "$statusCode")
                    return if (statusCode == 201) {
                        val entity = response.entity
                        val result = BufferedReader(InputStreamReader(entity.content))
                            .use(BufferedReader::readText).let {
                                Gson().fromJson(it, DataKeyResult::class.java)
                            }
                        EntityUtils.consume(entity)
                        result
                    }
                    else {
                        throw DataKeyServiceUnavailableException("Calling batchDataKey: dks_url: '$dksUrl', dks_correlation_id: '$dksCorrelationId' returned status_code '$statusCode'")
                    }
                }
            }
        }
        catch (ex: Exception) {
            when (ex) {
                is DataKeyServiceUnavailableException -> {
                    throw ex
                }
                else -> throw DataKeyServiceUnavailableException("Error contacting data key service: '$ex', dks_correlation_id: '$dksCorrelationId'")
            }
        }
    }

    override fun clearCache() {
        this.decryptedKeyCache = mutableMapOf()
    }

    private var decryptedKeyCache = mutableMapOf<String, String>()


    companion object {
        val logger = DataworksLogger.getLogger(HttpKeyService::class.toString())

        fun connect(): KeyService {
            return HttpKeyService(SecureHttpClientProvider.connect(), UUIDGeneratorImpl(), HttpClientProviderConfiguration.dksUrl)
        }
    }
}
