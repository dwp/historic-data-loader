package app.load.providers

import org.apache.http.client.config.RequestConfig
import org.apache.http.conn.ssl.SSLConnectionSocketFactory
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.http.impl.client.HttpClients
import org.apache.http.ssl.SSLContexts
import java.io.File
import javax.net.ssl.SSLContext

class SecureHttpClientProvider : HttpClientProvider {

    override fun client(): CloseableHttpClient =
        HttpClients.custom().run {
            setDefaultRequestConfig(requestConfig())
            setSSLSocketFactory(connectionFactory())
            build()
        }

    private fun requestConfig(): RequestConfig =
        RequestConfig.custom().run {
            setConnectTimeout(5_000)
            setConnectionRequestTimeout(5_000)
            build()
        }

    private fun connectionFactory() = SSLConnectionSocketFactory(
        sslContext(),
        arrayOf("TLSv1.2"),
        null,
        SSLConnectionSocketFactory.getDefaultHostnameVerifier())

    private fun sslContext(): SSLContext =
        SSLContexts.custom().run {
            loadKeyMaterial(
                File(identityStore),
                identityStorePassword.toCharArray(),
                identityKeyPassword.toCharArray()) { _, _ -> identityStoreAlias }
            loadTrustMaterial(File(trustStore), trustStorePassword.toCharArray())
            build()
        }

    private lateinit var identityStore: String

    private lateinit var identityStorePassword: String

    private lateinit var identityStoreAlias: String

    private lateinit var identityKeyPassword: String

    private lateinit var trustStore: String

    private lateinit var trustStorePassword: String
}
