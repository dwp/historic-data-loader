package app.load.mapreduce

import app.load.domain.DataKeyResult
import app.load.domain.EncryptionResult
import com.google.gson.Gson
import com.google.gson.JsonObject
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import org.everit.json.schema.loader.SchemaLoader
import org.json.JSONObject
import org.json.JSONTokener
import java.text.SimpleDateFormat
import java.util.*

class MessageProducerTest: StringSpec() {


    init {
        "fun testValidObjectGivesSchemaValidMessage()" {
            val messageProducer = MessageProducer()
            val validJson = validJsonOne()

            val jsonObject = Gson().fromJson(validJson, JsonObject::class.java)
            val id = Gson().toJson(jsonObject.getAsJsonObject("_id"))
            val encryptionResult = EncryptionResult(initialisationVector, encrypted)

            val dataKeyResult = DataKeyResult(dataKeyEncryptionKeyId, plaintextDataKey, ciphertextDataKey)
            val message = messageProducer.produceMessage(jsonObject!!, id, false, false, dateValue, "_lastModifiedDateTime", false, true, true, false, false, encryptionResult, dataKeyResult, database, collection)
            val actual = Gson().fromJson(message, JsonObject::class.java)
            val unitOfWorkId = actual["unitOfWorkId"]
            val timestamp = actual["timestamp"]
            val version = actual["version"]
            unitOfWorkId shouldNotBe null
            timestamp shouldNotBe null
            version shouldNotBe null
            actual.remove("unitOfWorkId")
            actual.remove("timestamp")
            actual.remove("version")
            validate(message)
            val expected = """{
              "traceId": "NOT_SET",
              "@type": "HDI",
              "message": {
                "@type": "MONGO_IMPORT",
                "_id": {
                  "idField": "$idFieldValue",
                  "anotherIdField": "$anotherIdFieldValue"
                },
                "mongo_format_stripped_from_id": false,
                "last_modified_date_time_was_altered": false,
                "created_date_time_was_altered": false,
                "removed_date_time_was_altered": true,
                "archived_date_time_was_altered": true,
                "historic_removed_record_altered_on_import": false,
                "historic_archived_record_altered_on_import": false,
                "_lastModifiedDateTime": "$dateValue",
                "timestamp_created_from": "_lastModifiedDateTime",
                "collection": "$collection",
                "db": "$database",
                "dbObject": "$encrypted",
                "encryption": {
                  "keyEncryptionKeyId": "$dataKeyEncryptionKeyId",
                  "initialisationVector": "$initialisationVector",
                  "encryptedEncryptionKey": "$ciphertextDataKey"
                }
              }
            }""".trimIndent()

            val expectedObject = Gson().fromJson(expected, JsonObject::class.java)
            actual shouldBe expectedObject
        }

        "fun testRemovedObjectGetsFlagAndType()" {

            val validJson = validDelete()

            val jsonObject = Gson().fromJson(validJson, JsonObject::class.java)
            val id = Gson().toJson(jsonObject.getAsJsonObject("_id"))
            val encryptionResult = EncryptionResult(initialisationVector, encrypted)

            val dataKeyResult = DataKeyResult(dataKeyEncryptionKeyId, plaintextDataKey, ciphertextDataKey)
            val message = MessageProducer().produceMessage(jsonObject!!, id, false, false, dateValue, "_lastModifiedDateTime", false, true, false, true, false, encryptionResult, dataKeyResult, database, collection)
            val actual = Gson().fromJson(message, JsonObject::class.java)
            val unitOfWorkId = actual["unitOfWorkId"]
            val timestamp = actual["timestamp"]
            unitOfWorkId shouldNotBe null
            timestamp shouldNotBe null
            val version = actual["version"]
            unitOfWorkId shouldNotBe null
            timestamp shouldNotBe null
            version shouldNotBe null
            actual.remove("unitOfWorkId")
            actual.remove("timestamp")
            actual.remove("version")
            validate(message)
            val expected = """{
              "traceId": "NOT_SET",
              "@type": "HDI",
              "message": {
                "@type": "MONGO_DELETE",
                "_id": {
                  "idField": "$idFieldValue",
                  "anotherIdField": "$anotherIdFieldValue"
                },
                "mongo_format_stripped_from_id": false,
                "last_modified_date_time_was_altered": false,
                "created_date_time_was_altered": false,
                "removed_date_time_was_altered": true,
                "archived_date_time_was_altered": false,
                "historic_removed_record_altered_on_import": true,
                "historic_archived_record_altered_on_import": false,
                "_lastModifiedDateTime": "$dateValue",
                "timestamp_created_from": "_lastModifiedDateTime",
                "collection": "$collection",
                "db": "$database",
                "dbObject": "$encrypted",
                "encryption": {
                  "keyEncryptionKeyId": "$dataKeyEncryptionKeyId",
                  "initialisationVector": "$initialisationVector",
                  "encryptedEncryptionKey": "$ciphertextDataKey"
                }
              }
            }""".trimIndent()

            val expectedObject = Gson().fromJson(expected, JsonObject::class.java)
            actual shouldBe expectedObject
        }

        "fun testArchivedObjectGetsFlagAndType()" {

            val validJson = validDelete()

            val jsonObject = Gson().fromJson(validJson, JsonObject::class.java)
            val id = Gson().toJson(jsonObject.getAsJsonObject("_id"))
            val encryptionResult = EncryptionResult(initialisationVector, encrypted)

            val dataKeyResult = DataKeyResult(dataKeyEncryptionKeyId, plaintextDataKey, ciphertextDataKey)
            val message = MessageProducer().produceMessage(jsonObject!!, id, false, false, dateValue, "_lastModifiedDateTime", false, false, true, false, true, encryptionResult, dataKeyResult, database, collection)
            val actual = Gson().fromJson(message, JsonObject::class.java)
            val unitOfWorkId = actual["unitOfWorkId"]
            val timestamp = actual["timestamp"]
            val version = actual["version"]
            unitOfWorkId shouldNotBe null
            timestamp shouldNotBe null
            version shouldNotBe null
            actual.remove("unitOfWorkId")
            actual.remove("timestamp")
            actual.remove("version")
            validate(message)
            val expected = """{
              "traceId": "NOT_SET",
              "@type": "HDI",
              "message": {
                "@type": "MONGO_DELETE",
                "_id": {
                  "idField": "$idFieldValue",
                  "anotherIdField": "$anotherIdFieldValue"
                },
                "mongo_format_stripped_from_id": false,
                "last_modified_date_time_was_altered": false,
                "created_date_time_was_altered": false,
                "removed_date_time_was_altered": false,
                "archived_date_time_was_altered": true,
                "historic_removed_record_altered_on_import": false,
                "historic_archived_record_altered_on_import": true,
                "_lastModifiedDateTime": "$dateValue",
                "timestamp_created_from": "_lastModifiedDateTime",
                "collection": "$collection",
                "db": "$database",
                "dbObject": "$encrypted",
                "encryption": {
                  "keyEncryptionKeyId": "$dataKeyEncryptionKeyId",
                  "initialisationVector": "$initialisationVector",
                  "encryptedEncryptionKey": "$ciphertextDataKey"
                }
              }
            }""".trimIndent()

            val expectedObject = Gson().fromJson(expected, JsonObject::class.java)
            actual shouldBe expectedObject
        }

        "fun testModifiedFieldsReflectedInMessage()" {

            val validJson = validJsonOne()

            val jsonObject = Gson().fromJson(validJson, JsonObject::class.java)
            val dateTime = "2019-11-13T14:02:03.000+0000"
            val encryptionResult = EncryptionResult(initialisationVector, encrypted)

            val dataKeyResult = DataKeyResult(dataKeyEncryptionKeyId, plaintextDataKey, ciphertextDataKey)
            val message = MessageProducer().produceMessage(jsonObject!!, idFieldValue, true, false, dateTime, "createdDateTime", true, false, false, false, false, encryptionResult, dataKeyResult, database, collection)
            val actual = Gson().fromJson(message, JsonObject::class.java)
            val unitOfWorkId = actual["unitOfWorkId"]
            val timestamp = actual["timestamp"]
            val version = actual["version"]
            unitOfWorkId shouldNotBe null
            timestamp shouldNotBe null
            version shouldNotBe null
            actual.remove("unitOfWorkId")
            actual.remove("timestamp")
            actual.remove("version")
            validate(message)
            val expected = """{
              "traceId": "NOT_SET",
              "@type": "HDI",
              "message": {
                "@type": "MONGO_IMPORT",
                "_id": "$idFieldValue",
                "mongo_format_stripped_from_id": false,
                "last_modified_date_time_was_altered": true,
                "created_date_time_was_altered": true,
                "removed_date_time_was_altered": false,
                "archived_date_time_was_altered": false,
                "historic_removed_record_altered_on_import": false,
                "historic_archived_record_altered_on_import": false,
                "_lastModifiedDateTime": "$dateTime",
                "timestamp_created_from": "createdDateTime",
                "collection": "$collection",
                "db": "$database",
                "dbObject": "$encrypted",
                "encryption": {
                  "keyEncryptionKeyId": "$dataKeyEncryptionKeyId",
                  "initialisationVector": "$initialisationVector",
                  "encryptedEncryptionKey": "$ciphertextDataKey"
                }
              }
            }""".trimIndent()

            val expectedObject = Gson().fromJson(expected, JsonObject::class.java)
            actual shouldBe expectedObject
        }

        "fun testModifiedFieldsReflectedInMessageWithStringId()" {

            val validJson = validJsonWithStringId()

            val jsonObject = Gson().fromJson(validJson, JsonObject::class.java)
            val id = "AN_ID"
            val dateTime = "2019-11-13T14:02:03.000+0000"
            val encryptionResult = EncryptionResult(initialisationVector, encrypted)

            val dataKeyResult = DataKeyResult(dataKeyEncryptionKeyId, plaintextDataKey, ciphertextDataKey)
            val message = MessageProducer().produceMessage(jsonObject!!, id, true, true, dateTime, "createdDateTime", true, false, false, false, false, encryptionResult, dataKeyResult, database, collection)
            val actual = Gson().fromJson(message, JsonObject::class.java)
            val unitOfWorkId = actual["unitOfWorkId"]
            val timestamp = actual["timestamp"]
            val version = actual["version"]
            unitOfWorkId shouldNotBe null
            timestamp shouldNotBe null
            version shouldNotBe null
            actual.remove("unitOfWorkId")
            actual.remove("timestamp")
            actual.remove("version")
            validate(message)
            val expected = """{
              "traceId": "NOT_SET",
              "@type": "HDI",
              "message": {
                "@type": "MONGO_IMPORT",
                "_id": "$id",
                "mongo_format_stripped_from_id": true,
                "last_modified_date_time_was_altered": true,
                "created_date_time_was_altered": true,
                "removed_date_time_was_altered": false,
                "archived_date_time_was_altered": false,
                "historic_removed_record_altered_on_import": false,
                "historic_archived_record_altered_on_import": false,
                "_lastModifiedDateTime": "$dateTime",
                "timestamp_created_from": "createdDateTime",
                "collection": "$collection",
                "db": "$database",
                "dbObject": "$encrypted",
                "encryption": {
                  "keyEncryptionKeyId": "$dataKeyEncryptionKeyId",
                  "initialisationVector": "$initialisationVector",
                  "encryptedEncryptionKey": "$ciphertextDataKey"
                }
              }
            }""".trimIndent()

            val expectedObject = Gson().fromJson(expected, JsonObject::class.java)
            actual shouldBe expectedObject
        }

        "fun testTwoObjectsGetSameGuid()" {
            unitOfWorkId(validJsonOne()) shouldBe unitOfWorkId(validJsonTwo())
        }

        "fun testTimestampRepresentsTimeOfMessageCreation()" {
            val start = Date()
            val timestamp = timestamp(validJsonOne())
            val date = SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS").parse(timestamp)
            val end = Date()
            (start.before(date) || start == date) shouldBe true
            (end.after(date) || end == date) shouldBe true
        }

        "fun testValidObjectWithTypeGivesSchemaValidMessage()" {

            val id = """{
                "idField": "$idFieldValue",
                "anotherIdField": "$anotherIdFieldValue"
            }"""

            val validJson = """{
            "_id": $id,
            "@type": "$type",
            "_lastModifiedDateTime": {
                "$dateKey": "$dateValue"
            }
        }""".trimIndent()

            val jsonObject = Gson().fromJson(validJson, JsonObject::class.java)
            val encryptionResult = EncryptionResult(initialisationVector, encrypted)

            val dataKeyResult = DataKeyResult(dataKeyEncryptionKeyId, plaintextDataKey, ciphertextDataKey)
            val message = MessageProducer().produceMessage(jsonObject, id, false, false, dateValue, "_lastModifiedDateTime", false, false, false, false, false, encryptionResult, dataKeyResult, database, collection)
            val actual = Gson().fromJson(message, JsonObject::class.java)
            val unitOfWorkId = actual["unitOfWorkId"]
            val timestamp = actual["timestamp"]
            val version = actual["version"]

            unitOfWorkId shouldNotBe null
            timestamp shouldNotBe null
            version shouldNotBe null
            actual.remove("unitOfWorkId")
            actual.remove("timestamp")
            actual.remove("version")
            validate(message)
            val expected = """{
              "traceId": "NOT_SET",
              "@type": "HDI",
              "message": {
                "@type": "$type",
                "_id": {
                  "idField": "$idFieldValue",
                  "anotherIdField": "$anotherIdFieldValue"
                },
                "mongo_format_stripped_from_id": false,
                "last_modified_date_time_was_altered": false,
                "created_date_time_was_altered": false,
                "removed_date_time_was_altered": false,
                "archived_date_time_was_altered": false,
                "historic_removed_record_altered_on_import": false,
                "historic_archived_record_altered_on_import": false,
                "_lastModifiedDateTime": "$dateValue",
                "timestamp_created_from": "_lastModifiedDateTime",
                "collection": "$collection",
                "db": "$database",
                "dbObject": "$encrypted",
                "encryption": {
                  "keyEncryptionKeyId": "$dataKeyEncryptionKeyId",
                  "initialisationVector": "$initialisationVector",
                  "encryptedEncryptionKey": "$ciphertextDataKey"
                }
              }
            }""".trimIndent()

            val expectedObject = Gson().fromJson(expected, JsonObject::class.java)
            actual shouldBe expectedObject
        }

    }


    private fun validate(json: String) = schemaLoader().load().build().validate(JSONObject(json))

    private fun schemaLoader() =
        SchemaLoader.builder()
            .schemaJson(schemaObject())
            .draftV7Support()
            .build()

    private fun schemaObject() =
        javaClass.getResourceAsStream("/message.schema.json")
            .use { inputStream ->
                JSONObject(JSONTokener(inputStream))
            }

    private fun validJsonTwo(): String {
        return """{
                "_id": {
                    "idField": "$anotherIdFieldValue",
                    "anotherIdField": "$idFieldValue"
                },
                "_lastModifiedDateTime": "$dateValue" 
            }""".trimIndent()
    }

    private fun validJsonOne(): String {
        return """{
                "_id": {
                    "idField": "$idFieldValue",
                    "anotherIdField": "$anotherIdFieldValue"
                },
                "_lastModifiedDateTime": "$dateValue" 
            }""".trimIndent()
    }

    private fun validJsonWithStringId(): String {
        return """{
                "_id": "$idFieldValue",
                "_lastModifiedDateTime": "$dateValue" 
            }""".trimIndent()
    }

    private fun validDelete(): String {
        return """{
                "_id": {
                    "idField": "$idFieldValue",
                    "anotherIdField": "$anotherIdFieldValue"
                },
                "@type": "MONGO_DELETE",
                "_lastModifiedDateTime": "$dateValue" 
            }""".trimIndent()
    }

    private fun unitOfWorkId(json: String) = messageField("unitOfWorkId", json)
    private fun timestamp(json: String) = messageField("timestamp", json)

    private fun messageField(field: String, json: String): String {
        val encryptionResult = EncryptionResult(initialisationVector, encrypted)
        val dataKeyResult = DataKeyResult(dataKeyEncryptionKeyId, plaintextDataKey, ciphertextDataKey)
        val jsonObjectOne = Gson().fromJson(json, JsonObject::class.java)
        val id = Gson().toJson(jsonObjectOne.getAsJsonObject("_id"))
        val messageOne = mp.produceMessage(jsonObjectOne, id, false, false, dateValue, "_lastModifiedDateTime", false, false, false, false, false, encryptionResult, dataKeyResult, database, collection)
        val actualOne = Gson().fromJson(messageOne, JsonObject::class.java)
        return actualOne.getAsJsonPrimitive(field).asString
    }

    private val mp = MessageProducer()
    private val dateKey = "\$date"
    private val dateValue = "2018-12-14T15:01:02.000+0000"
    private val idFieldValue = "idFieldValue"
    private val anotherIdFieldValue = "anotherIdFieldValue"
    private val initialisationVector = "initialisationVector"
    private val encrypted = "encrypted"
    private val dataKeyEncryptionKeyId = "cloudhsm:1,2"
    private val plaintextDataKey = "plaintextDataKey"
    private val ciphertextDataKey = "ciphertextDataKey"
    private val database = "database"
    private val collection = "collection"
    private val type = "type"
}
