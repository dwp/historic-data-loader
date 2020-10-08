package app.load.mapreduce

import app.load.domain.DataKeyResult
import app.load.domain.EncryptionResult
import com.google.gson.JsonObject
import com.jcabi.manifests.Manifests
import uk.gov.dwp.dataworks.logging.DataworksLogger
import java.io.File
import java.text.SimpleDateFormat
import java.util.*

class MessageProducer {

    fun produceMessage(jsonObject: JsonObject,
                       id: String,
                       idIsString: Boolean,
                       idWasModified: Boolean,
                       lastModifiedDateTime: String,
                       lastModifiedDateTimeSourceKey: String,
                       createdDateTimeWasModified: Boolean,
                       removedDateTimeWasModified: Boolean,
                       archivedDateTimeWasModified: Boolean,
                       isRemovedRecord: Boolean,
                       isArchivedRecord: Boolean,
                       encryptionResult: EncryptionResult,
                       dataKeyResult: DataKeyResult,
                       database: String,
                       collection: String): String {
        val type = jsonObject.getAsJsonPrimitive("@type")?.asString ?: "MONGO_IMPORT"
        val standardDateFormat = SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS")
        val timestamp = standardDateFormat.format(Date())
        val messageId = if (idIsString) """"$id"""" else id
        val lastModifiedDateTimeWasModified = lastModifiedDateTimeSourceKey != "_lastModifiedDateTime"

        return """{
   "unitOfWorkId": "$unitOfWorkId",
   "timestamp": "$timestamp",
   "traceId": "$correlationId",
   "@type": "HDI",
   "version": "$hdiVersion",
   "message": {
       "@type": "$type",
       "_id": $messageId,
       "mongo_format_stripped_from_id": $idWasModified,
       "last_modified_date_time_was_altered": $lastModifiedDateTimeWasModified,
       "created_date_time_was_altered": $createdDateTimeWasModified,
       "removed_date_time_was_altered": $removedDateTimeWasModified,
       "archived_date_time_was_altered": $archivedDateTimeWasModified,
       "historic_removed_record_altered_on_import": $isRemovedRecord,
       "historic_archived_record_altered_on_import": $isArchivedRecord,
       "_lastModifiedDateTime": "$lastModifiedDateTime",
       "timestamp_created_from": "$lastModifiedDateTimeSourceKey",
       "collection" : "$collection",
       "db": "$database",
       "dbObject": "${encryptionResult.encrypted}",
       "encryption": {
           "keyEncryptionKeyId": "${dataKeyResult.dataKeyEncryptionKeyId}",
           "initialisationVector": "${encryptionResult.initialisationVector}",
           "encryptedEncryptionKey": "${dataKeyResult.ciphertextDataKey}"
       }
   }
}"""
    }

    companion object {
        val logger = DataworksLogger.getLogger(MessageProducer::class.java.toString())
    }


    private val unitOfWorkId by lazy {
        UUID.randomUUID().toString()
    }

    private val correlationId by lazy {
        val correlationIdFile = System.getenv("CORRELATION_ID_FILE") ?: "/opt/emr/correlation_id.txt"
        if (File(correlationIdFile).exists()) {
            File(correlationIdFile).readText().trim()
        }
        else {
            "NOT_SET"
        }
    }

    private val hdiVersion: String by lazy {
        try {
            Manifests.read("Hdi-Version")
        }
        catch (e: Exception) {
            "NOT_SET"
        }
    }

}
