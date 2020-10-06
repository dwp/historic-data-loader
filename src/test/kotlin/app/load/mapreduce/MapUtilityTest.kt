package app.load.mapreduce

import app.load.domain.DataKeyResult
import app.load.domain.EncryptionResult
import app.load.services.CipherService
import com.google.gson.Gson
import com.google.gson.GsonBuilder
import com.google.gson.JsonPrimitive
import com.nhaarman.mockitokotlin2.*
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe

class MapUtilityTest: StringSpec() {
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

            val mapUtility = MapUtility(cipherService)

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

            Gson().fromJson(String(lineCaptor.firstValue), com.google.gson.JsonObject::class.java) shouldBe Gson().fromJson(expectedArgumentJson, com.google.gson.JsonObject::class.java)

        }

        "Id object returned as object" {
            val cipherService = mock<CipherService>()
            val mapUtility = MapUtility(cipherService)
            val id = com.google.gson.JsonObject()
            id.addProperty("key", "value")
            val expectedId = Gson().toJson(id)
            val (actualId, actualModified) = mapUtility.normalisedId(Gson(), id)
            expectedId shouldBe actualId
            actualModified shouldBe MapUtility.Companion.IdModification.UnmodifiedObjectId
        }

        "testIdObjectWithInnerCreatedDateInKafkaFormatReturnedAsObjectWithFlattenedDateInKafkaFormat" {
            testIdObjectWithInnerDateInKafkaFormatReturnedAsObjectWithFlattenedDateInKafkaFormat(MapUtility.CREATED_DATE_TIME_FIELD)
        }

        "testIdObjectWithInnerModifiedDateInKafkaFormatReturnedAsObjectWithFlattenedDateInKafkaFormat" {
            testIdObjectWithInnerDateInKafkaFormatReturnedAsObjectWithFlattenedDateInKafkaFormat(MapUtility.LAST_MODIFIED_DATE_TIME_FIELD)
        }

        "testIdObjectWithInnerRemovedDateInKafkaFormatReturnedAsObjectWithFlattenedDateInKafkaFormat" {
            testIdObjectWithInnerDateInKafkaFormatReturnedAsObjectWithFlattenedDateInKafkaFormat(MapUtility.REMOVED_DATE_TIME_FIELD)
        }

        "testIdObjectWithInnerArchivedDateInKafkaFormatReturnedAsObjectWithFlattenedDateInKafkaFormat" {
            testIdObjectWithInnerDateInKafkaFormatReturnedAsObjectWithFlattenedDateInKafkaFormat(MapUtility.ARCHIVED_DATE_TIME_FIELD)
        }

        "testIdObjectWithInnerCreatedDateInDumpFormatReturnedAsObjectWithFlattenedDateInKafkaFormat" {
            testIdObjectWithInnerDateInDumpFormatReturnedAsObjectWithFlattenedDateInKafkaFormat(MapUtility.CREATED_DATE_TIME_FIELD)
        }

        "testIdObjectWithInnerModifiedDateInDumpFormatReturnedAsObjectWithFlattenedDateInKafkaFormat" {
            testIdObjectWithInnerDateInDumpFormatReturnedAsObjectWithFlattenedDateInKafkaFormat(MapUtility.LAST_MODIFIED_DATE_TIME_FIELD)
        }

        "testIdObjectWithInnerRemovedDateInDumpFormatReturnedAsObjectWithFlattenedDateInKafkaFormat" {
            testIdObjectWithInnerDateInDumpFormatReturnedAsObjectWithFlattenedDateInKafkaFormat(MapUtility.REMOVED_DATE_TIME_FIELD)
        }

        "testIdObjectWithInnerArchivedDateInDumpFormatReturnedAsObjectWithFlattenedDateInKafkaFormat" {
            testIdObjectWithInnerDateInDumpFormatReturnedAsObjectWithFlattenedDateInKafkaFormat(MapUtility.ARCHIVED_DATE_TIME_FIELD)
        }

        "testIdObjectWithInnerCreatedDateStringReturnedUnchanged" {
            testIdObjectWithInnerDateStringReturnedUnchanged(MapUtility.CREATED_DATE_TIME_FIELD)
        }

        "testIdObjectWithInnerModifiedDateStringReturnedUnchanged" {
            testIdObjectWithInnerDateStringReturnedUnchanged(MapUtility.LAST_MODIFIED_DATE_TIME_FIELD)
        }

        "testIdObjectWithInnerRemovedDateStringReturnedUnchanged" {
            testIdObjectWithInnerDateStringReturnedUnchanged(MapUtility.REMOVED_DATE_TIME_FIELD)
        }

        "testIdObjectWithInnerArchivedDateStringReturnedUnchanged" {
            testIdObjectWithInnerDateStringReturnedUnchanged(MapUtility.ARCHIVED_DATE_TIME_FIELD)
        }

        "fun testIdStringReturnedAsString()" {
            val mapUtility = MapUtility(mock())
            val id = JsonPrimitive("id")
            val actual = mapUtility.normalisedId(Gson(), id)
            actual shouldBe Pair("id", MapUtility.Companion.IdModification.UnmodifiedStringId)
        }

        "fun testMongoIdStringReturnedAsString()" {
            val mapUtility = MapUtility(mock())
            val oid = com.google.gson.JsonObject()
            val oidValue = "OID_VALUE"
            oid.addProperty("\$oid", oidValue)
            val actual = mapUtility.normalisedId(Gson(), oid)
            actual shouldBe Pair(oidValue, MapUtility.Companion.IdModification.FlattenedMongoId)
        }

        "fun testIdNumberReturnedAsObject()" {
            val mapUtility = MapUtility(mock())
            val id = JsonPrimitive( 12345)
            val actual = mapUtility.normalisedId(Gson(), id)
            val expectedId = "12345"
            actual shouldBe Pair(expectedId, MapUtility.Companion.IdModification.UnmodifiedStringId)
        }

        "fun testIdArrayReturnedAsNull()" {
            val mapUtility = MapUtility(mock())
            val arrayValue = com.google.gson.JsonArray()
            arrayValue.add("1")
            arrayValue.add("2")
            val actual = mapUtility.normalisedId(Gson(), arrayValue)
            val expected = Pair("", MapUtility.Companion.IdModification.InvalidId)
            actual shouldBe expected
        }

        "fun testIdNullReturnedAsEmpty()" {
            val mapUtility = MapUtility(mock())
            val nullValue = com.google.gson.JsonNull.INSTANCE
            val actual = mapUtility.normalisedId(Gson(), nullValue)
            val expected = Pair("", MapUtility.Companion.IdModification.InvalidId)
            actual shouldBe expected
        }

        "fun testOverwriteFieldValueOverwritesCorrectValue()" {
            val mapUtility = MapUtility(mock())
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

        "fun testOverwriteFieldWithObjectOverwritesCorrectValue()" {
            val mapUtility = MapUtility(mock())
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

        "fun testCopyWhenFieldExistsInSourceButNotTarget()" {
            val mapUtility = MapUtility(mock())
            val sourceRecord = Gson().fromJson("""{ "SOURCE_KEY": "SOURCE_VALUE" }""", com.google.gson.JsonObject::class.java)
            val targetRecord = Gson().fromJson("""{ "TARGET_KEY": "TARGET_VALUE" }""", com.google.gson.JsonObject::class.java)
            val expected = Gson().fromJson("""{ "SOURCE_KEY": "SOURCE_VALUE", "TARGET_KEY": "TARGET_VALUE" }""", com.google.gson.JsonObject::class.java)
            mapUtility.copyField("SOURCE_KEY", sourceRecord, targetRecord)
            targetRecord shouldBe expected
        }

        "fun testCopyWhenFieldExistsInSourceAndTarget()" {
            val mapUtility = MapUtility(mock())
            val sourceRecord = Gson().fromJson("""{ "SHARED_KEY": "SOURCE_VALUE" }""", com.google.gson.JsonObject::class.java)
            val targetRecord = Gson().fromJson("""{ "SHARED_KEY": "TARGET_VALUE" }""", com.google.gson.JsonObject::class.java)
            val expected = Gson().fromJson("""{ "SHARED_KEY": "SOURCE_VALUE" }""", com.google.gson.JsonObject::class.java)
            mapUtility.copyField("SHARED_KEY", sourceRecord, targetRecord)
            targetRecord shouldBe expected
        }

        "fun testCopyWhenFieldNotInSource()" {
            val mapUtility = MapUtility(mock())
            val sourceRecord = Gson().fromJson("""{ "SOURCE_KEY": "SOURCE_VALUE" }""", com.google.gson.JsonObject::class.java)
            val targetRecord = Gson().fromJson("""{ "TARGET_KEY": "TARGET_VALUE" }""", com.google.gson.JsonObject::class.java)
            val expected = Gson().fromJson("""{ "TARGET_KEY": "TARGET_VALUE" }""", com.google.gson.JsonObject::class.java)
            mapUtility.copyField("ABSENT_KEY", sourceRecord, targetRecord)
            targetRecord shouldBe expected
        }

    }

    private fun testIdObjectWithInnerDateInKafkaFormatReturnedAsObjectWithFlattenedDateInKafkaFormat(dateField: String) {
        val mapUtility = MapUtility(mock())
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
        actualModified shouldBe MapUtility.Companion.IdModification.FlattenedInnerDate
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

        val mapUtility = MapUtility(mock())
        val (actualId, actualModified) =
                mapUtility.normalisedId(Gson(), Gson().fromJson(id, com.google.gson.JsonObject::class.java))

        val expectedId = """
            {
                "id": "ID",
                "$dateField": "2019-08-05T02:10:19.887+0000"
            }
        """.trimIndent()

        actualId shouldBe Gson().fromJson(expectedId, com.google.gson.JsonObject::class.java).toString()
        actualModified shouldBe MapUtility.Companion.IdModification.FlattenedInnerDate
    }

    private fun testIdObjectWithInnerDateStringReturnedUnchanged(dateField: String) {
        val mapUtility = MapUtility(mock())
        val id = """
            {
                "id": "ID",
                "$dateField": "EMBEDDED_DATE_FIELD"
            }
        """.trimIndent()

        val (actualId, actualModified) =
                mapUtility.normalisedId(Gson(), Gson().fromJson(id, com.google.gson.JsonObject::class.java))

        actualId shouldBe Gson().fromJson(id, com.google.gson.JsonObject::class.java).toString()
        actualModified shouldBe  MapUtility.Companion.IdModification.UnmodifiedObjectId
    }

}


// TODO: UCMapperTest and RecordReader test
//    @Test
//"Logs error for invalid json" {
//    val root = LoggerFactory.getLogger(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME) as ch.qos.logback.classic.Logger
//    val mockAppender: Appender<ILoggingEvent> = mock()
//    root.addAppender(mockAppender)
//    val dataKeyResult = DataKeyResult("dataKeyEncryptionKeyId", "plaintextDataKey", "ciphertextDataKey")
//    val encryptionResult = EncryptionResult("initialisationVector", "encrypted")
//
//    val cipherService = mock<CipherService> {
//        on { encrypt(any(), any())} doReturn encryptionResult
//    }
//
//    val invalidJson2 = """{"_id":{"declarationId":"87a4fad9-49af-4cb2-91b0-0056e2ac0eef"},"type":"addressDeclaration"""
//
//    val mapUtility = MapUtility(cipherService)
//    val gson = GsonBuilder().serializeNulls().create()
//
//    mapUtility.mappedRecord(gson, dataKeyResult, invalidJson2)
//    val captor = argumentCaptor<ILoggingEvent>()
//    verify(mockAppender, times(6)).doAppend(captor.capture())
//    val formattedMessages = captor.allValues.map { it.formattedMessage }
//    formattedMessages.contains("Error processing record\", \"line_number\":\"1\", \"file_name\":\"adb.collection.0001.json.gz.enc\", \"error_message\":\"parse error") shouldBe true
//}
//
//    @Test
//    fun should_Log_Error_For_Json_Without_Id() {
//
//        val root = LoggerFactory.getLogger(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME) as ch.qos.logback.classic.Logger
//        val mockAppender: Appender<ILoggingEvent> = mock()
//        root.addAppender(mockAppender)
//
//        val dataKeyResult = DataKeyResult("", "", "")
//        whenever(keyService.batchDataKey()).thenReturn(dataKeyResult)
//        val encryptionResult = EncryptionResult("", "")
//        whenever(cipherService.encrypt(any(), any())).thenReturn(encryptionResult)
//
//        whenever(messageUtils.parseGson(invalidJson2)).thenThrow(RuntimeException("parse error"))
//        val jsonObject = JsonObject()
//        whenever(messageUtils.parseGson(validJsonWithoutId)).thenReturn(com.google.gson.JsonObject())
//        whenever(messageUtils.getId(jsonObject)).thenReturn(null)
//        whenever(messageUtils.getId(jsonObject)).thenReturn(null)
//
//        whenever(messageUtils.getTimestampAsLong(any())).thenReturn(100)
//        val message = "message"
//        whenever(messageProducer.produceMessage(com.google.gson.JsonObject(), """{"key": "value"}""",
//                false, false, """{ "key": "value" }""", "_lastModifiedDateTime", false, false, false, false, false, encryptionResult, dataKeyResult, "adb",
//                "collection")).thenReturn(message)
//        val formattedKey = "0000-0000-00001"
//
//        whenever(messageUtils.generateKeyFromRecordBody(jsonObject)).thenReturn(formattedKey.toByteArray())
//
//        doNothing().whenever(MapUtility).ensureTable("adb:collection")
//        doNothing().whenever(MapUtility).putBatch(any(), any())
//
//        val data = listOf(invalidJson2, validJsonWithoutId)
//        val inputStreams = mutableListOf(getInputStream(data, validFileName))
//        mapUtility.write(inputStreams)
//
//        val captor = argumentCaptor<ILoggingEvent>()
//        verify(mockAppender, times(6)).doAppend(captor.capture())
//        val formattedMessages = captor.allValues.map { it.formattedMessage }
//
//        Assert.assertTrue(formattedMessages.contains("Error processing record\", \"line_number\":\"1\", \"file_name\":\"adb.collection.0001.json.gz.enc\", \"error_message\":\"parse error"))
//    }
//
//    @Test
//    fun should_Log_Error_And_Retry_10_Times_When_Streaming_Line_Of_File_Fails() {
//
//        val root = LoggerFactory.getLogger(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME) as ch.qos.logback.classic.Logger
//        val mockAppender: Appender<ILoggingEvent> = mock()
//        root.addAppender(mockAppender)
//
//        val dataKeyResult = DataKeyResult("", "", "")
//        whenever(keyService.batchDataKey()).thenReturn(dataKeyResult)
//
//        val inputStream = ByteArrayInputStream("""{ "_id": {"key": "value"}}""".toByteArray())
//        val s3InputStream = mock<S3ObjectInputStream>()
//
//        given(s3Service.objectInputStream("bucket", validFileName)).willReturn(s3InputStream)
//        doThrow(RuntimeException("RESET ERROR")).whenever(MapUtility).getBufferedReader(any())
//        doNothing().whenever(MapUtility).ensureTable("adb:collection")
//        doNothing().whenever(MapUtility).putBatch(any(), any())
//        val byteArray = """{ "_id": {"key": "value"}}""".toByteArray()
//        given(cipherService.decompressingDecryptingStream(any(), any(), any())).willReturn(ByteArrayInputStream(byteArray))
//        given(messageUtils.parseGson(any())).willReturn(Gson().fromJson("""{ "_id": {"key": "value"}}""", com.google.gson.JsonObject::class.java))
//        val key = mock<Key>()
//
//        val inputStreams = mutableListOf(DecompressedStream(inputStream, validFileName, key, ""))
//        mapUtility.write(inputStreams)
//        verify(cipherService, times(10)).decompressingDecryptingStream(any(), any(), any())
//
//        val captor = argumentCaptor<ILoggingEvent>()
//        verify(mockAppender, times(15)).doAppend(captor.capture())
//        val formattedMessages = captor.allValues.map { it.formattedMessage }
//
//        Assert.assertTrue(formattedMessages.contains("Error streaming file\", \"attempt_number\":\"1\", \"file_name\":\"$validFileName\", \"error_message\":\"RESET ERROR"))
//
//        for (i in 2..10) {
//            Assert.assertTrue(formattedMessages.contains("Error streaming file\", \"attempt_number\":\"$i\", \"file_name\":\"$validFileName\", \"error_message\":\"RESET ERROR"))
//        }
//    }
