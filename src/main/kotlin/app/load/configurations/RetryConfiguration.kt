package app.load.configurations

import java.io.File
import java.io.FileReader
import java.util.*

object RetryConfiguration {

    private val propertiesFile = System.getenv("RETRY_PROPERTIES_FILE") ?: "/opt/emr/retry.properties"

    private val properties: Properties by lazy {
        Properties().apply {
            if (File(propertiesFile).isFile) {
                load(FileReader(propertiesFile))
            }
        }
    }

    var retryMaxAttempts: Int = (properties.getProperty("retry.max.attempts") ?: "5").toInt()
    var retryInitialBackoff: Long = (properties.getProperty("retry.initial.backoff") ?: "1000").toLong()
    var retryBackoffMultiplier: Long = (properties.getProperty("retry.backoff.multiplier") ?: "1000").toLong()


}
