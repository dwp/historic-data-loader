package app.load.domain

import app.load.services.FilterService
import com.fasterxml.jackson.annotation.JsonIgnoreProperties

data class MappedRecord(val key: ByteArray, val wrapper: String, val version: Long, val filterStatus: FilterService.FilterStatus) {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false

        other as MappedRecord

        if (!key.contentEquals(other.key)) return false
        if (wrapper != other.wrapper) return false
        if (version != other.version) return false
        if (filterStatus != other.filterStatus) return false

        return true
    }

    override fun hashCode(): Int {
        var result = key.contentHashCode()
        result = 31 * result + wrapper.hashCode()
        result = 31 * result + version.hashCode()
        result = 31 * result + filterStatus.hashCode()
        return result
    }
}

@JsonIgnoreProperties(ignoreUnknown = true)
data class EncryptionMetadata(val keyEncryptionKeyId: String = "",
                              var plaintextDatakey: String = "",
                              val encryptedEncryptionKey: String = "",
                              val initialisationVector: String = "",
                              val keyEncryptionKeyHash: String = "",
                              val encryptionCipher: String = "",
                              val keyEncryptionCipher: String = "")

data class DataKeyResult(val dataKeyEncryptionKeyId: String, val plaintextDataKey: String, val ciphertextDataKey: String)
data class EncryptionResult(val initialisationVector: String, val encrypted: String)
