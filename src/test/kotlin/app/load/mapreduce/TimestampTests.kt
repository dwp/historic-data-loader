package app.load.mapreduce

import com.google.gson.Gson
import com.google.gson.JsonPrimitive
import com.nhaarman.mockitokotlin2.mock
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe
import java.text.SimpleDateFormat

class TimestampTests: StringSpec() {
    init {
        "fun testHasDateFieldOnPresenceOfMongoStyleDateField()" {
            val mapUtility = MapUtility(mock())
            val dateField = "\$date"
            val id = Gson().fromJson("""
                {
                    "id": "ID",
                    "createdDateTime": {
                        $dateField: "2019-08-05T02:10:19.887+0000"
                    }
                }
            """.trimIndent(), com.google.gson.JsonObject::class.java)
            mapUtility.hasDateField(id, "createdDateTime") shouldBe true
        }

        "fun Should_Parse_Valid_Incoming_Date_Format()" {
            val mapUtility = MapUtility(mock())
            val dateOne = "2019-12-14T15:01:02.000+0000"
            val df = SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZZZZ")
            val expected = df.parse(dateOne)
            val actual = mapUtility.getValidParsedDateTime(dateOne)
            actual shouldBe expected
        }

        "fun Should_Parse_Valid_Outgoing_Date_Format()" {
            val mapUtility = MapUtility(mock())
            val dateOne = "2019-12-14T15:01:02.000Z"
            val df = SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
            val expected = df.parse(dateOne)
            val actual = mapUtility.getValidParsedDateTime(dateOne)
            actual shouldBe expected
        }

        "fun shouldAcceptNonUtcPositiveOffsetTimeAndReturnAsUtc()" {
            val mapUtility = MapUtility(mock())
            val date = "2020-05-12T10:01:02.000+0100"
            val actual = mapUtility.kafkaDateFormat(date)
            val expected = "2020-05-12T09:01:02.000+0000"
            actual shouldBe expected
        }

        "fun shouldAcceptNonUtcNegativeOffsetTimeAndReturnAsUtc()" {
            val mapUtility = MapUtility(mock())
            val date = "2020-05-12T10:01:02.000-0100"
            val actual = mapUtility.kafkaDateFormat(date)
            val expected = "2020-05-12T11:01:02.000+0000"
            actual shouldBe expected
        }

        "fun shouldAcceptUtcTimeAndReturnAsUtc()" {
            val mapUtility = MapUtility(mock())
            val date = "2020-05-12T10:01:02.000+0000"
            val actual = mapUtility.kafkaDateFormat(date)
            val expected = "2020-05-12T10:01:02.000+0000"
            actual shouldBe expected
        }

        "fun shouldAcceptNonUtcTimeAndReturnAsUtc()" {
            val mapUtility = MapUtility(mock())
            val date = "2020-05-12T10:01:02.000+0100"
            val actual = mapUtility.kafkaDateFormat(date)
            val expected = "2020-05-12T09:01:02.000+0000"
            actual shouldBe expected
        }

        "fun Should_Throw_Error_With_Invalid_Date_Format()" {
            val mapUtility = MapUtility(mock())
            val exception = shouldThrow<Exception> {
                mapUtility.getValidParsedDateTime("2019-12-14T15:01:02")
            }
            exception.message shouldBe "Unparseable date found: '2019-12-14T15:01:02', did not match any supported date formats"
        }

        "fun testHasDateFieldReturnsFalseOnPresenceOfNonMongoStyleDateFieldWithExtraField()" {
            val mapUtility = MapUtility(mock())
            val dateField = "\$date"
            val id = Gson().fromJson("""
                {
                    "id": "ID",
                    "createdDateTime": {
                        $dateField: "2019-08-05T02:10:19.887+0000",
                        "additionalField": "ABC"
                    }
                }
            """.trimIndent(), com.google.gson.JsonObject::class.java)
            !mapUtility.hasDateField(id, "createdDateTime") shouldBe true
        }

        "fun testHasDateFieldReturnsFalseOnPresenceOfNonMongoStyleDateObjectField()" {
            val mapUtility = MapUtility(mock())
            val id = Gson().fromJson("""
                {
                    "id": "ID",
                    "createdDateTime": {
                        "additionalField": "ABC"
                    }
                }
            """.trimIndent(), com.google.gson.JsonObject::class.java)
            !mapUtility.hasDateField(id, "createdDateTime") shouldBe true
        }

        "fun testHasDateFieldReturnsFalseOnPresenceOfStringStyleDateField()" {
            val mapUtility = MapUtility(mock())
            val id = Gson().fromJson("""
                {
                    "id": "ID",
                    "createdDateTime": "2019-08-05T02:10:19.887+0000"
                }
            """.trimIndent(), com.google.gson.JsonObject::class.java)
            !mapUtility.hasDateField(id, "createdDateTime") shouldBe true
        }

        "fun testHasDateFieldReturnsFalseOnAbsenceOfDateField()" {
            val mapUtility = MapUtility(mock())
            val id = Gson().fromJson("""
                {
                    "id": "ID"
                }
            """.trimIndent(), com.google.gson.JsonObject::class.java)
            !mapUtility.hasDateField(id, "createdDateTime") shouldBe  true
        }

        "fun testLastModifiedDateTimeAsNonDateObjectReturnedAsCreated()" {
            val mapUtility = MapUtility(mock())
            val lastModified = com.google.gson.JsonObject()
            val lastModifiedValue = "testDateField"
            lastModified.addProperty("\$notDate", lastModifiedValue)
            val actual = mapUtility.lastModifiedDateTime(lastModified, "CREATED_TIMESTAMP")
            val expected = Pair("CREATED_TIMESTAMP", "createdDateTime")
            actual shouldBe expected
        }

        "fun testLastModifiedDateTimeAsDateObjectInDumpFormatReturnedAsDateFieldValueInKafkaFormat()" {
            val mapUtility = MapUtility(mock())
            val lastModified = com.google.gson.JsonObject()
            val lastModifiedValue = "2019-08-05T02:10:19.887Z"
            lastModified.addProperty("\$date", lastModifiedValue)
            val actual = mapUtility.lastModifiedDateTime(lastModified, "CREATED_TIMESTAMP")
            val expected = Pair("2019-08-05T02:10:19.887+0000", "_lastModifiedDateTimeStripped")
            actual shouldBe expected
        }

        "fun testLastModifiedDateTimeAsDateObjectInKafkaFormatReturnedAsDateFieldValueInKafkaFormat()" {
            val mapUtility = MapUtility(mock())
            val lastModified = com.google.gson.JsonObject()
            val lastModifiedValue = "2019-08-05T02:10:19.887+0000"
            lastModified.addProperty("\$date", lastModifiedValue)
            val actual = mapUtility.lastModifiedDateTime(lastModified, "CREATED_TIMESTAMP")
            val expected = Pair("2019-08-05T02:10:19.887+0000", "_lastModifiedDateTimeStripped")
            actual shouldBe expected
        }

        "fun testLastModifiedDateTimeAsStringReturnedAsValue()" {
            val mapUtility = MapUtility(mock())
            val lastModified = JsonPrimitive("testDateString")
            val actual = mapUtility.lastModifiedDateTime(lastModified, "CREATED_TIMESTAMP")
            val expected = Pair("testDateString", "_lastModifiedDateTime")
            actual shouldBe expected
        }

        "fun testLastModifiedDateTimeArrayReturnedAsCreatedWhenCreatedNotBlank()" {
            val mapUtility = MapUtility(mock())
            val arrayValue = com.google.gson.JsonArray()
            arrayValue.add("1")
            arrayValue.add("2")
            val actual = mapUtility.lastModifiedDateTime(arrayValue, "")
            val expected = Pair(MapUtility.EPOCH, "epoch")
            actual shouldBe expected
        }

        "fun testLastModifiedDateTimeNullReturnedAsCreatedWhenCreatedNotBlank()" {
            val mapUtility = MapUtility(mock())
            val nullValue = com.google.gson.JsonNull.INSTANCE
            val actual = mapUtility.lastModifiedDateTime(nullValue, "CREATED_TIMESTAMP")
            val expected = Pair("CREATED_TIMESTAMP", "createdDateTime")
            actual shouldBe expected
        }

        "fun testLastModifiedDateTimeNullReturnedAsEpochWhenCreatedBlank()" {
            val mapUtility = MapUtility(mock())
            val nullValue = com.google.gson.JsonNull.INSTANCE
            val actual = mapUtility.lastModifiedDateTime(nullValue, "")
            val expected = Pair(MapUtility.EPOCH, "epoch")
            actual shouldBe expected
        }

        "fun testAbsentOptionalDateTimeAsObjectReturnedAsBlank()" {
            val mapUtility = MapUtility(mock())
            val message = com.google.gson.JsonObject()
            val fieldName = "_optionalDateTime"
            message.addProperty("otherProperty", "123")
            val actual = mapUtility.optionalDateTime(fieldName, message)
            val expected = Pair("", false)
            actual shouldBe expected
        }

        "fun testOptionalDateTimeInDumpFormatReturnedAsStringInKafkaFormat()" {
            val mapUtility = MapUtility(mock())
            val optionalDateTimeField = com.google.gson.JsonObject()
            val optionalDateTimeValue = "2019-08-05T02:10:19.887Z"
            optionalDateTimeField.addProperty("\$date", optionalDateTimeValue)
            val message = com.google.gson.JsonObject()
            val fieldName = "_optionalDateTime"
            message.add(fieldName, optionalDateTimeField)
            val actual = mapUtility.optionalDateTime(fieldName, message)
            val expected = Pair("2019-08-05T02:10:19.887+0000", true)
            actual shouldBe expected
        }

        "fun testOptionalDateTimeInKafkaFormatReturnedAsStringInKafkaFormat()" {
            val mapUtility = MapUtility(mock())
            val optionalDateTimeField = com.google.gson.JsonObject()
            val optionalDateTimeValue = "2019-08-05T02:10:19.887+0000"
            optionalDateTimeField.addProperty("\$date", optionalDateTimeValue)
            val message = com.google.gson.JsonObject()
            val fieldName = "_optionalDateTime"
            message.add(fieldName, optionalDateTimeField)
            val actual = mapUtility.optionalDateTime(fieldName, message)
            val expected = Pair(optionalDateTimeValue, true)
            actual shouldBe expected
        }

        "fun testOptionalDateTimeAsStringReturnedAsString()" {
            val mapUtility = MapUtility(mock())
            val optionalDateTimeValue = "DATE_FIELD_VALUE"
            val message = com.google.gson.JsonObject()
            val fieldName = "_optionalDateTime"
            message.addProperty(fieldName, optionalDateTimeValue)
            val actual = mapUtility.optionalDateTime(fieldName, message)
            val expected = Pair(optionalDateTimeValue, false)
            actual shouldBe expected
        }

        "fun testInvalidOptionalDateTimeAsObjectReturnedAsBlank()" {
            val mapUtility = MapUtility(mock())
            val optionalDateTimeField = com.google.gson.JsonObject()
            val optionalDateTimeValue = "DATE_FIELD_VALUE"
            optionalDateTimeField.addProperty("\$invalidProperty", optionalDateTimeValue)
            val message = com.google.gson.JsonObject()
            val fieldName = "_optionalDateTime"
            message.add(fieldName, optionalDateTimeField)
            val actual = mapUtility.optionalDateTime(fieldName, message)
            val expected = Pair("", true)
            actual shouldBe expected
        }

    }
}
