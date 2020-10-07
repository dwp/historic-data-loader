package app.load.mapreduce

import app.load.domain.DataKeyResult
import app.load.domain.MappedRecord
import com.google.gson.Gson

interface MapUtility {
    fun mappedRecord(gson: Gson, dataKeyResult: DataKeyResult, lineFromDump: String): MappedRecord
}
