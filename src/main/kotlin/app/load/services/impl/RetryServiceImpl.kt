package app.load.services.impl

import app.load.configurations.RetryConfiguration
import app.load.domain.DataKeyResult
import app.load.exceptions.DataKeyServiceUnavailableException
import app.load.services.KeyService
import app.load.services.RetryService
import uk.gov.dwp.dataworks.logging.DataworksLogger

class RetryServiceImpl(private val keyService: KeyService,
                       private val retryMaxAttempts: Int,
                       private val retryInitialBackoff: Long,
                       private val retryBackoffMultiplier: Long) : RetryService {


    @Throws(DataKeyServiceUnavailableException::class)
    override fun batchDataKey() = retry { keyService.batchDataKey() } as DataKeyResult

    @Throws(DataKeyServiceUnavailableException::class)
    override fun decryptKey(encryptionKeyId: String, encryptedKey: String) =
            retry { keyService.decryptKey(encryptionKeyId, encryptedKey) } as String

    private fun retry(func: () -> Any): Any {

        var success = false
        var attempts = 0
        var exception: Exception? = null
        var result: Any? = null

        while (!success && attempts < retryMaxAttempts) {
            try {
                result = func()
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

    companion object {
        private val logger = DataworksLogger.getLogger(RetryServiceImpl::class.java.toString())
        fun connect()=
            RetryServiceImpl(HttpKeyService.connect(),
                    RetryConfiguration.retryMaxAttempts,
                    RetryConfiguration.retryInitialBackoff,
                    RetryConfiguration.retryBackoffMultiplier)
    }
}
