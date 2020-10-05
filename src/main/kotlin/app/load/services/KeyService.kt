package app.load.services

import app.load.domain.DataKeyResult

interface KeyService {
    fun batchDataKey(): DataKeyResult
    fun decryptKey(encryptionKeyId: String, encryptedKey: String): String
    fun clearCache()
}
