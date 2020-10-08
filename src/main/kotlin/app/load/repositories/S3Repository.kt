package app.load.repositories

import app.load.configurations.AwsConfiguration
import app.load.configurations.S3Configuration
import com.amazonaws.ClientConfiguration
import com.amazonaws.Protocol
import com.amazonaws.auth.AWSStaticCredentialsProvider
import com.amazonaws.auth.BasicAWSCredentials
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain
import com.amazonaws.client.builder.AwsClientBuilder
import com.amazonaws.services.s3.AmazonS3
import com.amazonaws.services.s3.AmazonS3ClientBuilder
import com.amazonaws.services.s3.model.ListObjectsV2Request
import com.amazonaws.services.s3.model.S3ObjectSummary

class S3Repository(private val amazonS3: AmazonS3,
                   private val bucket: String,
                   private val objectPrefixes: String,
                   private val topicName: String) {

    fun allObjectSummaries()=
            objectPrefixes.split(",").filter(String::isNotBlank).map(::objectSummaries).flatten()

    private fun objectSummaries(prefix: String, objectSummaries: MutableList<S3ObjectSummary> = mutableListOf(), nextContinuationToken: String = ""):
            List<S3ObjectSummary> {
        val request = listObjectsRequest(prefix, nextContinuationToken)
        val objectListing = amazonS3.listObjectsV2(request)
        val filenameRe = Regex("""/\Q${topicName.replace(Regex("""^db\."""), "")}\E.\d+\.json\.gz\.enc$""")
        objectSummaries.addAll(objectListing.objectSummaries.filter { topicName.isBlank() || filenameRe.find(it.key) != null })

        if (objectListing != null && !objectListing.isTruncated) {
            return objectSummaries
        }

        return objectSummaries(prefix, objectSummaries, objectListing.nextContinuationToken)
    }

    private fun listObjectsRequest(objectPrefix: String, nextContinuationToken: String) =
            ListObjectsV2Request().apply {
                bucketName = bucket
                prefix = objectPrefix
                if (nextContinuationToken.isNotBlank()) {
                    continuationToken = nextContinuationToken
                }
            }

    companion object {
        fun connect() = S3Repository(amazonS3, S3Configuration.bucket, S3Configuration.prefix, S3Configuration.topicName)
        private val amazonS3: AmazonS3 by lazy {
            if (AwsConfiguration.useLocalStack) {
                AmazonS3ClientBuilder.standard().run {
                    withEndpointConfiguration(AwsClientBuilder.EndpointConfiguration("http://aws:4566", "eu-west-2"))
                    withClientConfiguration(ClientConfiguration().apply {
                        withProtocol(Protocol.HTTP)
                    })
                    withCredentials(AWSStaticCredentialsProvider(BasicAWSCredentials("accessKey", "secretKey")))
                    withPathStyleAccessEnabled(true)
                    disableChunkedEncoding()
                    build()
                }
            } else {
                AmazonS3ClientBuilder.standard().run {
                    withCredentials(DefaultAWSCredentialsProviderChain())
                    withRegion(AwsConfiguration.region)
                    withClientConfiguration(ClientConfiguration().apply {
                        maxConnections = S3Configuration.maxConnections
                    })
                    build()
                }
            }
        }
    }
}
