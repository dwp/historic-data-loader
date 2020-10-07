package app.load.services

import app.load.domain.DataKeyResult

interface RetryService {
    fun batchDataKey(): DataKeyResult
    fun decryptKey(encryptionKeyId: String, encryptedKey: String): String
}
