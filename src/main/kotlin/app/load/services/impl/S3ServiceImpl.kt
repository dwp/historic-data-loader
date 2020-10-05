package app.load.services.impl

import app.load.services.S3Service
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.model.S3ObjectInputStream
import uk.gov.dwp.dataworks.logging.DataworksLogger

class S3ServiceImpl(private val amazonS3: AmazonS3): S3Service {

    override fun objectInputStream(bucket: String, key: String): S3ObjectInputStream
            = amazonS3.getObject(bucket, key).objectContent


    companion object {
        val logger = DataworksLogger.getLogger(S3Service::class.java.toString())
        const val maxAttempts = 10
        const val initialBackoffMillis = 1000L
        const val backoffMultiplier = 2.0
    }
}
