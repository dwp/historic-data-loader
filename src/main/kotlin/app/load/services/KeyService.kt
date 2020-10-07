package app.load.services

import app.load.domain.DataKeyResult
import app.load.exceptions.DataKeyServiceUnavailableException

interface KeyService {
    @Throws(DataKeyServiceUnavailableException::class)
    fun batchDataKey(): DataKeyResult
    @Throws(DataKeyServiceUnavailableException::class)
    fun decryptKey(encryptionKeyId: String, encryptedKey: String): String
    fun clearCache()
}
