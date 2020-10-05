package app.load.providers

import app.load.configurations.HttpClientProviderConfiguration
import org.apache.http.client.config.RequestConfig
import org.apache.http.conn.ssl.SSLConnectionSocketFactory
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.http.impl.client.HttpClients
import org.apache.http.ssl.SSLContexts
import java.io.BufferedReader
import java.io.File
import java.io.FileReader
import java.security.Provider
import java.security.Security
import javax.net.ssl.SSLContext

class SecureHttpClientProvider(private val identityStore: String,
                               private val identityStorePassword: String,
                               private val identityStoreAlias: String,
                               private val identityKeyPassword: String,
                               private val trustStore: String,
                               private val trustStorePassword: String) : HttpClientProvider {

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

    companion object {
        fun connect() =
                SecureHttpClientProvider(HttpClientProviderConfiguration.identityStore,
                        HttpClientProviderConfiguration.identityStorePassword,
                        HttpClientProviderConfiguration.identityStoreAlias,
                        HttpClientProviderConfiguration.identityKeyPassword,
                        HttpClientProviderConfiguration.trustStore,
                        HttpClientProviderConfiguration.trustStorePassword)
    }
}
