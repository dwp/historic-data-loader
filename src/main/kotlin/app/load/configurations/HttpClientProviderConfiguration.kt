package app.load.configurations

import java.io.File
import java.io.FileReader
import java.util.*

object HttpClientProviderConfiguration {

    private val properties = Properties().apply {
        if (File("/opt/emr/dks.properties").exists()) {
            load(FileReader("/opt/emr/dks.properties"))
        }
    }

    val identityStore: String = properties["identity.keystore"] as String? ?: ""
    val identityStorePassword: String = properties["identity.store.password"] as String? ?: ""
    val identityStoreAlias: String = properties["identity.store.alias"] as String? ?: ""
    val identityKeyPassword: String = properties["identity.key.password"] as String? ?: ""
    val trustStore: String = properties["trust.keystore"] as String? ?: ""
    val trustStorePassword: String = properties["trust.store.password"] as String? ?: ""
    val dksUrl = properties["data.key.service.url"] as String? ?: ""

}
