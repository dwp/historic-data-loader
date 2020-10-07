package app.load.mapreduce

import app.load.configurations.RetryConfiguration
import app.load.domain.DataKeyResult
import app.load.exceptions.DataKeyServiceUnavailableException
import app.load.services.KeyService
import app.load.services.impl.HttpKeyService
import uk.gov.dwp.dataworks.logging.DataworksLogger

class RetryUtilityImpl(private val keyService: KeyService,
                       private val retryMaxAttempts: Int,
                       private val retryInitialBackoff: Long,
                       private val retryBackoffMultiplier: Long) : RetryUtility {

    @Throws(DataKeyServiceUnavailableException::class)
    override fun batchDataKey(): DataKeyResult {

        var success = false
        var attempts = 0
        var exception: Exception? = null
        var result: DataKeyResult? = null

        while (!success && attempts < retryMaxAttempts) {
            try {
                result = keyService.batchDataKey()
                success = true
            } catch (e: Exception) {

                val delay = if (attempts == 0) retryInitialBackoff
                else (retryInitialBackoff * attempts * retryBackoffMultiplier.toFloat()).toLong()

                logger.warn("Failed to fetch datakey",
                        "attempt_number" to "${attempts + 1}",
                        "max_attempts" to "${retryMaxAttempts}",
                        "retry_delay" to "$delay", "error_message" to "${e.message}")
                Thread.sleep(delay)
                exception = e
            } finally {
                attempts++
            }
        }

        if (!success) {
            if (exception != null) {
                throw exception
            }
        }

        return result!!
    }

//    @Throws(DataKeyServiceUnavailableException::class)
//    fun decryptKey(encryptionKeyId: String, encryptedKey: String): String
//    }

    companion object {
        private val logger = DataworksLogger.getLogger(RetryUtilityImpl::class.java.toString())

        fun connect()=
            RetryUtilityImpl(HttpKeyService.connect(),
                    RetryConfiguration.retryMaxAttempts,
                    RetryConfiguration.retryInitialBackoff,
                    RetryConfiguration.retryBackoffMultiplier)
    }
}
