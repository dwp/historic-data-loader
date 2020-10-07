package app.load.mapreduce

import app.load.domain.DataKeyResult

interface RetryUtility {
    fun batchDataKey(): DataKeyResult
}
