package app.load.mapreduce

import app.load.domain.DataKeyResult
import app.load.domain.EncryptionResult
import app.load.services.CipherService
import app.load.services.FilterService
import com.google.gson.Gson
import com.google.gson.GsonBuilder
import com.google.gson.JsonPrimitive
import com.nhaarman.mockitokotlin2.*
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe

class MapUtilityImplTest: StringSpec() {
    init {
        "Object is updated prior to encryption" {
            val date = "\$date"
            val validJson = """{
                |    "_id": {
                |        "declarationId":"87a4fad9-49af-4cb2-91b0-0056e2ac0eef",
                |        "createdDateTime": {
                |           "$date": "2000-01-01T00:00:00.000Z" 
                |        }
                |    },
                |    "nullfield": null,
                |    "type":"addressDeclaration",
                |    "_lastModifiedDateTime": {
                |           "$date": "2010-01-01T00:00:00.000Z" 
                |    }
                |}""".trimMargin()

            val json = Gson().fromJson(validJson, com.google.gson.JsonObject::class.java)
            val dumpLine = json.toString()
            val gson = GsonBuilder().serializeNulls().create()
            val dataKeyResult = DataKeyResult("dataKeyEncryptionKeyId", "plaintextDataKey", "ciphertextDataKey")
            val encryptionResult = EncryptionResult("initialisationVector", "encrypted")

            val cipherService = mock<CipherService> {
                on { encrypt(any(), any()) } doReturn encryptionResult
            }

            val filterService = mock<FilterService> {
                on { filterStatus(any())} doReturn FilterService.FilterStatus.DoNotFilter
            }

            val mapUtility = MapUtilityImpl(cipherService, filterService)

            mapUtility.mappedRecord(gson, dataKeyResult, dumpLine)
            val dataKeyCaptor = argumentCaptor<String>()
            val lineCaptor = argumentCaptor<ByteArray>()
            verify(cipherService, times(1)).encrypt(dataKeyCaptor.capture(), lineCaptor.capture())
            val expectedArgumentJson = """{
                |    "type":"addressDeclaration",
                |    "_id": {
                |        "declarationId":"87a4fad9-49af-4cb2-91b0-0056e2ac0eef",
                |        "createdDateTime": "2000-01-01T00:00:00.000+0000" 
                |    },
                |    "nullfield": null,
                |    "_lastModifiedDateTime": "2010-01-01T00:00:00.000+0000"
                |}""".trimMargin()

            Gson().fromJson(String(lineCaptor.firstValue), com.google.gson.JsonObject::class.java) shouldBe
                    Gson().fromJson(expectedArgumentJson, com.google.gson.JsonObject::class.java)

        }

        "Id object returned as object" {
            val cipherService = mock<CipherService>()
            val filterService = mock<FilterService>()
            val mapUtility = MapUtilityImpl(cipherService, filterService)
            val id = com.google.gson.JsonObject()
            id.addProperty("key", "value")
            val expectedId = Gson().toJson(id)
            val (actualId, actualModified) = mapUtility.normalisedId(Gson(), id)
            expectedId shouldBe actualId
            actualModified shouldBe MapUtilityImpl.Companion.IdModification.UnmodifiedObjectId
        }

        "testIdObjectWithInnerCreatedDateInKafkaFormatReturnedAsObjectWithFlattenedDateInKafkaFormat" {
            testIdObjectWithInnerDateInKafkaFormatReturnedAsObjectWithFlattenedDateInKafkaFormat(MapUtilityImpl.CREATED_DATE_TIME_FIELD)
        }

        "testIdObjectWithInnerModifiedDateInKafkaFormatReturnedAsObjectWithFlattenedDateInKafkaFormat" {
            testIdObjectWithInnerDateInKafkaFormatReturnedAsObjectWithFlattenedDateInKafkaFormat(MapUtilityImpl.LAST_MODIFIED_DATE_TIME_FIELD)
        }

        "testIdObjectWithInnerRemovedDateInKafkaFormatReturnedAsObjectWithFlattenedDateInKafkaFormat" {
            testIdObjectWithInnerDateInKafkaFormatReturnedAsObjectWithFlattenedDateInKafkaFormat(MapUtilityImpl.REMOVED_DATE_TIME_FIELD)
        }

        "testIdObjectWithInnerArchivedDateInKafkaFormatReturnedAsObjectWithFlattenedDateInKafkaFormat" {
            testIdObjectWithInnerDateInKafkaFormatReturnedAsObjectWithFlattenedDateInKafkaFormat(MapUtilityImpl.ARCHIVED_DATE_TIME_FIELD)
        }

        "testIdObjectWithInnerCreatedDateInDumpFormatReturnedAsObjectWithFlattenedDateInKafkaFormat" {
            testIdObjectWithInnerDateInDumpFormatReturnedAsObjectWithFlattenedDateInKafkaFormat(MapUtilityImpl.CREATED_DATE_TIME_FIELD)
        }

        "testIdObjectWithInnerModifiedDateInDumpFormatReturnedAsObjectWithFlattenedDateInKafkaFormat" {
            testIdObjectWithInnerDateInDumpFormatReturnedAsObjectWithFlattenedDateInKafkaFormat(MapUtilityImpl.LAST_MODIFIED_DATE_TIME_FIELD)
        }

        "testIdObjectWithInnerRemovedDateInDumpFormatReturnedAsObjectWithFlattenedDateInKafkaFormat" {
            testIdObjectWithInnerDateInDumpFormatReturnedAsObjectWithFlattenedDateInKafkaFormat(MapUtilityImpl.REMOVED_DATE_TIME_FIELD)
        }

        "testIdObjectWithInnerArchivedDateInDumpFormatReturnedAsObjectWithFlattenedDateInKafkaFormat" {
            testIdObjectWithInnerDateInDumpFormatReturnedAsObjectWithFlattenedDateInKafkaFormat(MapUtilityImpl.ARCHIVED_DATE_TIME_FIELD)
        }

        "testIdObjectWithInnerCreatedDateStringReturnedUnchanged" {
            testIdObjectWithInnerDateStringReturnedUnchanged(MapUtilityImpl.CREATED_DATE_TIME_FIELD)
        }

        "testIdObjectWithInnerModifiedDateStringReturnedUnchanged" {
            testIdObjectWithInnerDateStringReturnedUnchanged(MapUtilityImpl.LAST_MODIFIED_DATE_TIME_FIELD)
        }

        "testIdObjectWithInnerRemovedDateStringReturnedUnchanged" {
            testIdObjectWithInnerDateStringReturnedUnchanged(MapUtilityImpl.REMOVED_DATE_TIME_FIELD)
        }

        "testIdObjectWithInnerArchivedDateStringReturnedUnchanged" {
            testIdObjectWithInnerDateStringReturnedUnchanged(MapUtilityImpl.ARCHIVED_DATE_TIME_FIELD)
        }

        "testIdStringReturnedAsString" {
            val mapUtility = MapUtilityImpl(mock(), mock())
            val id = JsonPrimitive("id")
            val actual = mapUtility.normalisedId(Gson(), id)
            actual shouldBe Pair("id", MapUtilityImpl.Companion.IdModification.UnmodifiedStringId)
        }

        "testMongoIdStringReturnedAsString" {
            val mapUtility = MapUtilityImpl(mock(), mock())
            val oid = com.google.gson.JsonObject()
            val oidValue = "OID_VALUE"
            oid.addProperty("\$oid", oidValue)
            val actual = mapUtility.normalisedId(Gson(), oid)
            actual shouldBe Pair(oidValue, MapUtilityImpl.Companion.IdModification.FlattenedMongoId)
        }

        "testIdNumberReturnedAsObject" {
            val mapUtility = MapUtilityImpl(mock(), mock())
            val id = JsonPrimitive( 12345)
            val actual = mapUtility.normalisedId(Gson(), id)
            val expectedId = "12345"
            actual shouldBe Pair(expectedId, MapUtilityImpl.Companion.IdModification.UnmodifiedStringId)
        }

        "testIdArrayReturnedAsNull" {
            val mapUtility = MapUtilityImpl(mock(), mock())
            val arrayValue = com.google.gson.JsonArray()
            arrayValue.add("1")
            arrayValue.add("2")
            val actual = mapUtility.normalisedId(Gson(), arrayValue)
            val expected = Pair("", MapUtilityImpl.Companion.IdModification.InvalidId)
            actual shouldBe expected
        }

        "testIdNullReturnedAsEmpty" {
            val mapUtility = MapUtilityImpl(mock(), mock())
            val nullValue = com.google.gson.JsonNull.INSTANCE
            val actual = mapUtility.normalisedId(Gson(), nullValue)
            val expected = Pair("", MapUtilityImpl.Companion.IdModification.InvalidId)
            actual shouldBe expected
        }

        "testOverwriteFieldValueOverwritesCorrectValue" {
            val mapUtility = MapUtilityImpl(mock(), mock())
            val id = "OID_WRENCHED_FROM_MONGO_ID"
            val lastModifiedDateTime = "DATETIME_WRENCHED_FROM_MONGO_ID"
            val lastModifiedDateTimeNew = "NEW_DATETIME"
            val obj = com.google.gson.JsonObject()
            obj.addProperty("_id", id)
            obj.addProperty("_lastModifiedDateTime", lastModifiedDateTime)
            obj.addProperty("other", "TEST")
            val expected = com.google.gson.JsonObject()
            expected.addProperty("_id", id)
            expected.addProperty("_lastModifiedDateTime", lastModifiedDateTimeNew)
            expected.addProperty("other", "TEST")
            val actual = mapUtility.overwriteFieldValue("_lastModifiedDateTime", lastModifiedDateTimeNew, obj)
            actual shouldBe expected
        }

        "testOverwriteFieldWithObjectOverwritesCorrectValue" {
            val mapUtility = MapUtilityImpl(mock(), mock())
            val id = Gson().fromJson("""{
            |    "key1": "val1",
            |    "key2": "val2"
            |}""".trimMargin(), com.google.gson.JsonObject::class.java)


            val obj = Gson().fromJson("""{
            |    "_id": "OLD_ID",
            |    "other_field": "OTHER_FIELD_VALUE"
            |}""".trimMargin(), com.google.gson.JsonObject::class.java)

            val actual = mapUtility.overwriteFieldValueWithObject("_id", id, obj)

            val expected = Gson().fromJson("""{
            |   "_id": {
            |       "key1": "val1",
            |       "key2": "val2"
            |   },
            |   "other_field": "OTHER_FIELD_VALUE"
            }""".trimMargin(), com.google.gson.JsonObject::class.java)

            actual shouldBe expected
        }

        "testCopyWhenFieldExistsInSourceButNotTarget" {
            val mapUtility = MapUtilityImpl(mock(), mock())
            val sourceRecord = Gson().fromJson("""{ "SOURCE_KEY": "SOURCE_VALUE" }""", com.google.gson.JsonObject::class.java)
            val targetRecord = Gson().fromJson("""{ "TARGET_KEY": "TARGET_VALUE" }""", com.google.gson.JsonObject::class.java)
            val expected = Gson().fromJson("""{ "SOURCE_KEY": "SOURCE_VALUE", "TARGET_KEY": "TARGET_VALUE" }""", com.google.gson.JsonObject::class.java)
            mapUtility.copyField("SOURCE_KEY", sourceRecord, targetRecord)
            targetRecord shouldBe expected
        }

        "testCopyWhenFieldExistsInSourceAndTarget" {
            val mapUtility = MapUtilityImpl(mock(), mock())
            val sourceRecord = Gson().fromJson("""{ "SHARED_KEY": "SOURCE_VALUE" }""", com.google.gson.JsonObject::class.java)
            val targetRecord = Gson().fromJson("""{ "SHARED_KEY": "TARGET_VALUE" }""", com.google.gson.JsonObject::class.java)
            val expected = Gson().fromJson("""{ "SHARED_KEY": "SOURCE_VALUE" }""", com.google.gson.JsonObject::class.java)
            mapUtility.copyField("SHARED_KEY", sourceRecord, targetRecord)
            targetRecord shouldBe expected
        }

        "testCopyWhenFieldNotInSource" {
            val mapUtility = MapUtilityImpl(mock(), mock())
            val sourceRecord = Gson().fromJson("""{ "SOURCE_KEY": "SOURCE_VALUE" }""", com.google.gson.JsonObject::class.java)
            val targetRecord = Gson().fromJson("""{ "TARGET_KEY": "TARGET_VALUE" }""", com.google.gson.JsonObject::class.java)
            val expected = Gson().fromJson("""{ "TARGET_KEY": "TARGET_VALUE" }""", com.google.gson.JsonObject::class.java)
            mapUtility.copyField("ABSENT_KEY", sourceRecord, targetRecord)
            targetRecord shouldBe expected
        }

    }

    private fun testIdObjectWithInnerDateInKafkaFormatReturnedAsObjectWithFlattenedDateInKafkaFormat(dateField: String) {
        val mapUtility = MapUtilityImpl(mock(), mock())
        val dateInnerField = "\$date"

        val id = """
            {
                "id": "ID",
                "$dateField": {
                    $dateInnerField: "2019-08-05T02:10:19.887+0000"
                }
            }
        """.trimIndent()

        val originalId = Gson().fromJson(id, com.google.gson.JsonObject::class.java)
        val copyOfOriginalId = originalId.deepCopy()
        val (actualId, actualModified) = mapUtility.normalisedId(Gson(), originalId)

        val expectedId = """
            {
                "id": "ID",
                "$dateField": "2019-08-05T02:10:19.887+0000"
            }
        """.trimIndent()

        actualId shouldBe Gson().fromJson(expectedId, com.google.gson.JsonObject::class.java).toString()
        actualModified shouldBe MapUtilityImpl.Companion.IdModification.FlattenedInnerDate
        copyOfOriginalId shouldBe originalId
    }

    private fun testIdObjectWithInnerDateInDumpFormatReturnedAsObjectWithFlattenedDateInKafkaFormat(dateField: String) {
        val innerDateField = "\$date"

        val id = """
            {
                "id": "ID",
                "$dateField": {
                    $innerDateField: "2019-08-05T02:10:19.887Z"
                }
            }
        """.trimIndent()

        val mapUtility = MapUtilityImpl(mock(), mock())
        val (actualId, actualModified) =
                mapUtility.normalisedId(Gson(), Gson().fromJson(id, com.google.gson.JsonObject::class.java))

        val expectedId = """
            {
                "id": "ID",
                "$dateField": "2019-08-05T02:10:19.887+0000"
            }
        """.trimIndent()

        actualId shouldBe Gson().fromJson(expectedId, com.google.gson.JsonObject::class.java).toString()
        actualModified shouldBe MapUtilityImpl.Companion.IdModification.FlattenedInnerDate
    }

    private fun testIdObjectWithInnerDateStringReturnedUnchanged(dateField: String) {
        val mapUtility = MapUtilityImpl(mock(), mock())
        val id = """
            {
                "id": "ID",
                "$dateField": "EMBEDDED_DATE_FIELD"
            }
        """.trimIndent()

        val (actualId, actualModified) =
                mapUtility.normalisedId(Gson(), Gson().fromJson(id, com.google.gson.JsonObject::class.java))

        actualId shouldBe Gson().fromJson(id, com.google.gson.JsonObject::class.java).toString()
        actualModified shouldBe  MapUtilityImpl.Companion.IdModification.UnmodifiedObjectId
    }

}
