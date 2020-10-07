package app.load.mapreduce

import app.load.domain.DataKeyResult
import app.load.domain.EncryptionResult
import app.load.domain.MappedRecord
import app.load.exceptions.InvalidRecordException
import app.load.services.CipherService
import app.load.services.FilterService
import com.google.gson.Gson
import com.google.gson.JsonElement
import com.google.gson.JsonObject
import org.apache.commons.lang3.StringUtils
import uk.gov.dwp.dataworks.logging.DataworksLogger
import java.text.SimpleDateFormat
import java.util.*

class MapUtilityImpl(private val cipherService: CipherService, private val filterService: FilterService) : MapUtility {
    override fun mappedRecord(gson: Gson, dataKeyResult: DataKeyResult, lineFromDump: String): MappedRecord {
        val (lineAsJsonBeforeReFormatting, recordIsRemovedRecord) = reformatRemoved(gson, lineFromDump)
        val (lineAsJson, recordIsArchivedRecord) = reformatArchived(gson, lineAsJsonBeforeReFormatting)

        val originalId = lineAsJson.get("_id") ?: throw InvalidRecordException("Encountered line without id")
        val (id, idModificationType) = normalisedId(gson, originalId)

        val (createdDateTime, createdDateTimeWasModified) = optionalDateTime(CREATED_DATE_TIME_FIELD, lineAsJson)
        val (removedDateTime, removedDateTimeWasModified) = optionalDateTime(REMOVED_DATE_TIME_FIELD, lineAsJson)
        val (archivedDateTime, archivedDateTimeWasModified) = optionalDateTime(ARCHIVED_DATE_TIME_FIELD, lineAsJson)

        val originalLastModifiedDateTime = lineAsJson.get(LAST_MODIFIED_DATE_TIME_FIELD)
        val (lastModifiedDateTime, lastModifiedDateTimeSourceField)
                = lastModifiedDateTime(originalLastModifiedDateTime, createdDateTime)

        var updatedLineAsJson = lineAsJson
        if (idModificationType == IdModification.FlattenedMongoId) {
            updatedLineAsJson = overwriteFieldValue("_id", id, updatedLineAsJson)
        }
        else if (idModificationType == IdModification.FlattenedInnerDate) {
            updatedLineAsJson = overwriteFieldValueWithObject("_id", gson.fromJson(id, JsonObject::class.java), updatedLineAsJson)
        }

        if (lastModifiedDateTimeSourceField != LAST_MODIFIED_DATE_TIME_FIELD) {
            updatedLineAsJson = overwriteFieldValue(LAST_MODIFIED_DATE_TIME_FIELD, lastModifiedDateTime, updatedLineAsJson)
        }

        if (createdDateTimeWasModified) {
            updatedLineAsJson = overwriteFieldValue(CREATED_DATE_TIME_FIELD, createdDateTime, updatedLineAsJson)
        }

        if (removedDateTimeWasModified) {
            updatedLineAsJson = overwriteFieldValue(REMOVED_DATE_TIME_FIELD, removedDateTime, updatedLineAsJson)
        }

        if (archivedDateTimeWasModified) {
            updatedLineAsJson = overwriteFieldValue(ARCHIVED_DATE_TIME_FIELD, archivedDateTime, updatedLineAsJson)
        }

        val encryptionResult = encryptDbObject(dataKeyResult.plaintextDataKey, gson.toJson(updatedLineAsJson))
        val idWasModified = (idModificationType == IdModification.FlattenedMongoId  ||
                idModificationType == IdModification.FlattenedInnerDate)

        val idIsString = (idModificationType == IdModification.UnmodifiedStringId) ||
                (idModificationType == IdModification.FlattenedMongoId)

        println("LINE: '$lineFromDump'.")
        println("lastModifiedDateTimeSourceField: '$lastModifiedDateTimeSourceField'.")


        val messageWrapper = messageProducer.produceMessage(updatedLineAsJson, id,
                idIsString,
                idWasModified,
                lastModifiedDateTime,
                lastModifiedDateTimeSourceField,
                StringUtils.isNotBlank(createdDateTime) && createdDateTimeWasModified,
                StringUtils.isNotBlank(removedDateTime) && removedDateTimeWasModified,
                StringUtils.isNotBlank(archivedDateTime) && archivedDateTimeWasModified,
                recordIsRemovedRecord,
                recordIsArchivedRecord,
                encryptionResult,
                dataKeyResult,
                UcRecordReader.database,
                UcRecordReader.collection)

        val messageJsonObject = messageUtils.parseJson(messageWrapper)
        val lastModifiedTimestampLong = messageUtils.getTimestampAsLong(lastModifiedDateTime)
        val formattedKey = messageUtils.generateKeyFromRecordBody(messageJsonObject)

        val filterStatus = filterService.filterStatus(lastModifiedTimestampLong)
        return MappedRecord(formattedKey, messageWrapper, lastModifiedTimestampLong, filterStatus)
    }

    private fun encryptDbObject(dataKey: String, line: String): EncryptionResult {
        return cipherService.encrypt(dataKey, line.toByteArray())
    }

    fun reformatRemoved(gson: Gson, recordFromDump: String): Pair<JsonObject, Boolean> {
        val record = messageUtils.parseGson(recordFromDump)

        return if (record.has(REMOVED_RECORD_FIELD)) {
            val removedRecord = deepCopy(gson, record.getAsJsonObject(REMOVED_RECORD_FIELD), JsonObject::class.java)
            copyField(LAST_MODIFIED_DATE_TIME_FIELD, record, removedRecord)
            copyField(REMOVED_DATE_TIME_FIELD, record, removedRecord)
            copyField(TIMESTAMP_FIELD, record, removedRecord)
            removedRecord.addProperty("@type", MONGO_DELETE)
            Pair(deepCopy(gson, removedRecord, JsonObject::class.java), true)
        } else {
            Pair(record, false)
        }
    }

    fun reformatArchived(gson: Gson, record: JsonObject): Pair<JsonObject, Boolean> {
        return if (record.has(ARCHIVED_RECORD_FIELD)) {
            val archivedRecord = deepCopy(gson, record.getAsJsonObject(ARCHIVED_RECORD_FIELD), JsonObject::class.java)
            copyField(LAST_MODIFIED_DATE_TIME_FIELD, record, archivedRecord)
            copyField(ARCHIVED_DATE_TIME_FIELD, record, archivedRecord)
            copyField(TIMESTAMP_FIELD, record, archivedRecord)
            archivedRecord.addProperty("@type", MONGO_DELETE)
            Pair(deepCopy(gson, archivedRecord, JsonObject::class.java), true)
        } else {
            Pair(record, false)
        }
    }

    fun normalisedId(gson: Gson, id: JsonElement?): Pair<String, IdModification> {
        if (id != null) {
            return if (id.isJsonObject) {
                val obj = deepCopy(gson, id.asJsonObject!!, JsonObject::class.java)
                if (obj.entrySet().size == 1 && obj["\$oid"] != null && obj["\$oid"].isJsonPrimitive) {
                    Pair(obj["\$oid"].asJsonPrimitive.asString, IdModification.FlattenedMongoId)
                }
                else if (hasKnownDateField(obj)) {
                    var flattened = flattenedDateField(obj, CREATED_DATE_TIME_FIELD)
                    flattened = flattenedDateField(flattened, LAST_MODIFIED_DATE_TIME_FIELD)
                    flattened = flattenedDateField(flattened, REMOVED_DATE_TIME_FIELD)
                    flattened = flattenedDateField(flattened, ARCHIVED_DATE_TIME_FIELD)
                    Pair(gson.toJson(flattened), IdModification.FlattenedInnerDate)
                }
                else {
                    Pair(gson.toJson(id.asJsonObject), IdModification.UnmodifiedObjectId)
                }
            }
            else if (id.isJsonPrimitive) {
                Pair(id.asJsonPrimitive.asString, IdModification.UnmodifiedStringId)
            }
            else {
                Pair("", IdModification.InvalidId)
            }
        }
        else {
            return Pair("", IdModification.InvalidId)
        }
    }

    fun lastModifiedDateTime(incomingDateTime: JsonElement?, createdDateTime: String): Pair<String, String> {

        val fallBackDate = if (StringUtils.isNotBlank(createdDateTime)) createdDateTime else EPOCH
        val fallBackField = if (fallBackDate == EPOCH) EPOCH_FIELD else CREATED_DATE_TIME_FIELD

        if (incomingDateTime != null) {
            when {
                incomingDateTime.isJsonObject -> {
                    val obj = incomingDateTime.asJsonObject!!
                    return if (obj.entrySet().size == 1 && obj["\$date"] != null && obj["\$date"].isJsonPrimitive) {
                        Pair(kafkaDateFormat(obj["\$date"].asJsonPrimitive.asString), LAST_MODIFIED_DATE_TIME_FIELD_STRIPPED)
                    }
                    else {
                        logger.debug("_lastModifiedDateTime was an object, without a \$date field",
                                "incoming_value" to "$incomingDateTime", "outgoing_value" to fallBackDate)
                        Pair(fallBackDate, fallBackField)
                    }
                }
                incomingDateTime.isJsonPrimitive -> {
                    val outgoingValue = incomingDateTime.asJsonPrimitive.asString
                    logger.debug("$LAST_MODIFIED_DATE_TIME_FIELD was a string", "incoming_value" to "$incomingDateTime", "outgoing_value" to outgoingValue)
                    return Pair(outgoingValue, LAST_MODIFIED_DATE_TIME_FIELD)
                }
                else -> {
                    logger.debug("Invalid $LAST_MODIFIED_DATE_TIME_FIELD object", "incoming_value" to "$incomingDateTime", "outgoing_value" to fallBackDate)
                    return Pair(fallBackDate, fallBackField)
                }
            }
        }
        else {
            logger.debug("No incoming $LAST_MODIFIED_DATE_TIME_FIELD object", "incoming_value" to "$incomingDateTime", "outgoing_value" to fallBackDate)
            return Pair(fallBackDate, fallBackField)
        }
    }

    fun optionalDateTime(name: String, parent: JsonObject): Pair<String, Boolean> {
        val incomingDateTime = parent.get(name)
        if (incomingDateTime != null) {
            when {
                incomingDateTime.isJsonObject -> {
                    val obj = incomingDateTime.asJsonObject!!
                    return if (obj.entrySet().size == 1 && obj["\$date"] != null && obj["\$date"].isJsonPrimitive) {
                        Pair(kafkaDateFormat(obj["\$date"].asJsonPrimitive.asString), true)
                    }
                    else {
                        Pair("", true)
                    }
                }
                incomingDateTime.isJsonPrimitive -> {
                    val outgoingValue = incomingDateTime.asJsonPrimitive.asString
                    return Pair(outgoingValue, false)
                }
                else -> {
                    logger.warn("Invalid $name object", "incoming_value" to "$incomingDateTime", "outgoing_value" to "")
                    return Pair("", true)
                }
            }
        }
        else {
            return Pair("", false)
        }
    }

    fun overwriteFieldValue(fieldKey: String, fieldValue: String, json: JsonObject): JsonObject {
        json.remove(fieldKey)
        json.addProperty(fieldKey, fieldValue)
        return json
    }

    fun overwriteFieldValueWithObject(fieldKey: String, fieldValue: JsonElement, json: JsonObject): JsonObject {
        json.remove(fieldKey)
        json.add(fieldKey, fieldValue)
        return json
    }

    fun copyField(fieldName: String, sourceRecord: JsonObject, targetRecord: JsonObject) {
        if (sourceRecord.has(fieldName)) {
            if (targetRecord.has(fieldName)) {
                targetRecord.remove(fieldName)
            }
            targetRecord.add(fieldName, sourceRecord.get(fieldName))
        }
    }

    fun kafkaDateFormat(input: String): String {
        val parsedDateTime = getValidParsedDateTime(input)
        val df = SimpleDateFormat(VALID_OUTGOING_DATE_FORMAT)
        df.timeZone = TimeZone.getTimeZone("UTC")
        return df.format(parsedDateTime)
    }

    fun getValidParsedDateTime(timestampAsString: String): Date {
        VALID_DATE_FORMATS.forEach {
            try {
                val df = SimpleDateFormat(it)
                df.timeZone = TimeZone.getTimeZone("UTC")
                return df.parse(timestampAsString)
            } catch (e: Exception) {
                // do nothing
            }
        }
        throw Exception("Unparseable date found: '$timestampAsString', did not match any supported date formats")
    }


    private fun hasKnownDateField(obj: JsonObject) = hasDateField(obj, CREATED_DATE_TIME_FIELD) ||
            hasDateField(obj, LAST_MODIFIED_DATE_TIME_FIELD) ||
            hasDateField(obj, REMOVED_DATE_TIME_FIELD) ||
            hasDateField(obj, ARCHIVED_DATE_TIME_FIELD)

    private fun flattenedDateField(obj: JsonObject, dateField: String): JsonObject {
        if (hasDateField(obj, dateField)) {
            val dateString = obj[dateField].asJsonObject["\$date"].asString
            obj.remove(dateField)
            obj.addProperty(dateField, kafkaDateFormat(dateString))
        }

        return obj
    }

    fun hasDateField(obj: JsonObject, dateField: String) =
            obj[dateField] != null &&
                    obj[dateField].isJsonObject &&
                    obj[dateField].asJsonObject.entrySet().size == 1 &&
                    obj[dateField].asJsonObject["\$date"] != null &&
                    obj[dateField].asJsonObject["\$date"].isJsonPrimitive

    private fun <T> deepCopy(gson: Gson, obj: T, type: Class<T>?): T = gson.fromJson(gson.toJson(obj, type), type)

    companion object {
        val logger = DataworksLogger.getLogger(MapUtilityImpl::class.java.toString())
        const val LAST_MODIFIED_DATE_TIME_FIELD = "_lastModifiedDateTime"
        const val CREATED_DATE_TIME_FIELD = "createdDateTime"
        const val REMOVED_DATE_TIME_FIELD = "_removedDateTime"
        const val ARCHIVED_DATE_TIME_FIELD = "_archivedDateTime"
        const val EPOCH = "1980-01-01T00:00:00.000+0000"

        const val MONGO_DELETE = "MONGO_DELETE"

        private const val VALID_INCOMING_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
        const val VALID_OUTGOING_DATE_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSZZZZ"
        val VALID_DATE_FORMATS = listOf(VALID_INCOMING_DATE_FORMAT, VALID_OUTGOING_DATE_FORMAT)

        private const val LAST_MODIFIED_DATE_TIME_FIELD_STRIPPED = "_lastModifiedDateTimeStripped"
        private const val EPOCH_FIELD = "epoch"
        private const val REMOVED_RECORD_FIELD = "_removed"
        private const val ARCHIVED_RECORD_FIELD = "_archived"
        private const val TIMESTAMP_FIELD = "timestamp"

        private val messageProducer = MessageProducer()
        private val messageUtils = MessageUtils()

        enum class IdModification {
            UnmodifiedObjectId,
            UnmodifiedStringId,
            FlattenedMongoId,
            FlattenedInnerDate,
            InvalidId
        }
    }

}
