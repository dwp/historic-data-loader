package app.load.mapreduce

import com.google.gson.Gson
import com.nhaarman.mockitokotlin2.mock
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe

class ReformattingTests: StringSpec() {
    init {
        "fun testReformatNonRemovedReturnsUnmodifiedRecordWhenNoRemovedElementExists()" {
            val mapUtility = MapUtility(mock())
            val innerRecord = """{ "_id": "123456789" }""".trimIndent()
            val recordWithRemovedElement = """{ "_notRemoved": $innerRecord }"""
            val (actual, wasChanged) = mapUtility.reformatRemoved(Gson(), recordWithRemovedElement)
            val expected = Gson().fromJson(recordWithRemovedElement, com.google.gson.JsonObject::class.java)
            actual shouldBe expected 
            wasChanged shouldBe false
        }

        "fun testReformatRemovedReturnsInnerRecordWhenRemovedElementExists()" {
            val mapUtility = MapUtility(mock())
            val innerRecord = """{ "_id": "123456789" }""".trimIndent()
            val recordWithRemovedElement = """{ "_removed": $innerRecord }"""
            val (actual, wasChanged) = mapUtility.reformatRemoved(Gson(), recordWithRemovedElement)
            val expected = Gson().fromJson(innerRecord, com.google.gson.JsonObject::class.java)
            expected.addProperty("@type", "MONGO_DELETE")
            actual shouldBe expected
            wasChanged shouldBe true
        }

        "fun testReformatRemovedOverwritesTypeAttribute()" {
            val mapUtility = MapUtility(mock())
            val innerRecord = """{ "_id": "123456789", "@type": "MONGO_INSERT" }"""
            val recordWithRemovedElement = """{ "_removed": $innerRecord }"""
            val (actual, wasChanged) = mapUtility.reformatRemoved(Gson(), recordWithRemovedElement)
            val expected = Gson().fromJson("""{ "_id": "123456789", "@type": "MONGO_DELETE" }""", com.google.gson.JsonObject::class.java)
            actual shouldBe expected
            wasChanged shouldBe true
        }

        "fun testReformatRemovedReturnsOuterRecordWhenRemovedElementDoesNotExist()" {
            val mapUtility = MapUtility(mock())
            val outerRecord = """{ "_id": "123456789" }""".trimIndent()
            val (actual, wasChanged) = mapUtility.reformatRemoved(Gson(), outerRecord)
            val expected = Gson().fromJson(outerRecord, com.google.gson.JsonObject::class.java)
            actual shouldBe expected
            wasChanged shouldBe false
        }

        "fun testReformatTransplantsLastModifiedWhenRemovedElementExists()" {
            val mapUtility = MapUtility(mock())
            val innerRecord = """{ "_id": "123456789" }""".trimIndent()
            val innerRecordWithTimestamp = """{ "_id": "123456789", "_lastModifiedDateTime": "OUTER_LAST_MODIFIED" }""".trimIndent()
            val recordWithRemovedElement = """{ "_removed": $innerRecord, "_lastModifiedDateTime": "OUTER_LAST_MODIFIED" }"""
            val (actual, wasChanged) = mapUtility.reformatRemoved(Gson(), recordWithRemovedElement)
            val expected = Gson().fromJson(innerRecordWithTimestamp, com.google.gson.JsonObject::class.java)
            expected.addProperty("@type", "MONGO_DELETE")
            actual shouldBe expected
            wasChanged shouldBe true
        }

        "fun testReformatReplacesLastModifiedWhenRemovedElementExists()" {
            val mapUtility = MapUtility(mock())
            val innerRecord = """{ "_id": "123456789", "_lastModifiedDateTime": "INNER_LAST_MODIFIED" }""".trimIndent()
            val innerRecordWithTimestamp = """{ "_id": "123456789", "_lastModifiedDateTime": "OUTER_LAST_MODIFIED" }""".trimIndent()
            val recordWithRemovedElement = """{ "_removed": $innerRecord, "_lastModifiedDateTime": "OUTER_LAST_MODIFIED" }"""
            val (actual, wasChanged) = mapUtility.reformatRemoved(Gson(), recordWithRemovedElement)
            val expected = Gson().fromJson(innerRecordWithTimestamp, com.google.gson.JsonObject::class.java)
            expected.addProperty("@type", "MONGO_DELETE")
            actual shouldBe expected
            wasChanged shouldBe true
        }

        "fun testReformatTransplantsRemovedTimeWhenRemovedElementExists()" {
            val mapUtility = MapUtility(mock())
            val innerRecord = """{ "_id": "123456789" }""".trimIndent()
            val innerRecordWithTimestamp = """{ "_id": "123456789", "_removedDateTime": "OUTER_REMOVED_TIME" }""".trimIndent()
            val recordWithRemovedElement = """{ "_removed": $innerRecord, "_removedDateTime": "OUTER_REMOVED_TIME" }"""
            val (actual, wasChanged) = mapUtility.reformatRemoved(Gson(), recordWithRemovedElement)
            val expected = Gson().fromJson(innerRecordWithTimestamp, com.google.gson.JsonObject::class.java)
            expected.addProperty("@type", "MONGO_DELETE")
            actual shouldBe expected
            wasChanged shouldBe true
        }

        "fun testReformatReplacesRemovedTimeWhenRemovedElementExists()" {
            val mapUtility = MapUtility(mock())
            val innerRecord = """{ "_id": "123456789", "_removedDateTime": "INNER_REMOVED_TIME" }""".trimIndent()
            val innerRecordWithTimestamp = """{ "_id": "123456789", "_removedDateTime": "OUTER_REMOVED_TIME" }""".trimIndent()
            val recordWithRemovedElement = """{ "_removed": $innerRecord, "_removedDateTime": "OUTER_REMOVED_TIME" }"""
            val (actual, wasChanged) = mapUtility.reformatRemoved(Gson(), recordWithRemovedElement)
            val expected = Gson().fromJson(innerRecordWithTimestamp, com.google.gson.JsonObject::class.java)
            expected.addProperty("@type", "MONGO_DELETE")
            actual shouldBe expected
            wasChanged shouldBe true
        }

        "fun testReformatTransplantsTimestampWhenRemovedElementExists()" {
            val mapUtility = MapUtility(mock())
            val innerRecord = """{ "_id": "123456789" }""".trimIndent()
            val innerRecordWithTimestamp = """{ "_id": "123456789", "timestamp": "OUTER_TIMESTAMP" }""".trimIndent()
            val recordWithRemovedElement = """{ "_removed": $innerRecord, "timestamp": "OUTER_TIMESTAMP" }"""
            val (actual, wasChanged) = mapUtility.reformatRemoved(Gson(), recordWithRemovedElement)
            val expected = Gson().fromJson(innerRecordWithTimestamp, com.google.gson.JsonObject::class.java)
            expected.addProperty("@type", "MONGO_DELETE")
            actual shouldBe expected
            wasChanged shouldBe true
        }

        "fun testReformatReplacesTimestampWhenRemovedElementExists()" {
            val mapUtility = MapUtility(mock())
            val innerRecord = """{ "_id": "123456789", "timestamp": "INNER_TIMESTAMP" }""".trimIndent()
            val innerRecordWithTimestamp = """{ "_id": "123456789", "timestamp": "OUTER_TIMESTAMP" }""".trimIndent()
            val recordWithRemovedElement = """{ "_removed": $innerRecord, "timestamp": "OUTER_TIMESTAMP" }"""
            val (actual, wasChanged) = mapUtility.reformatRemoved(Gson(), recordWithRemovedElement)
            val expected = Gson().fromJson(innerRecordWithTimestamp, com.google.gson.JsonObject::class.java)
            expected.addProperty("@type", "MONGO_DELETE")
            actual shouldBe expected
            wasChanged shouldBe true
        }

        "fun testReformatNonArchivedReturnsUnmodifiedRecordWhenNoArchivedElementExists()" {
            val mapUtility = MapUtility(mock())
            val innerRecord = """{ "_id": "123456789" }""".trimIndent()
            val recordWithArchivedElement = """{ "_notArchived": $innerRecord }"""
            val json = Gson().fromJson(recordWithArchivedElement, com.google.gson.JsonObject::class.java)
            val (actual, wasChanged) = mapUtility.reformatArchived(Gson(), json)
            val expected = Gson().fromJson(recordWithArchivedElement, com.google.gson.JsonObject::class.java)
            actual shouldBe expected
            wasChanged shouldBe false
        }

        "fun testReformatArchivedReturnsInnerRecordWhenArchivedElementExists()" {
            val mapUtility = MapUtility(mock())
            val innerRecord = """{ "_id": "123456789" }""".trimIndent()
            val recordWithArchivedElement = """{ "_archived": $innerRecord }"""
            val json = Gson().fromJson(recordWithArchivedElement, com.google.gson.JsonObject::class.java)
            val (actual, wasChanged) = mapUtility.reformatArchived(Gson(), json)
            val expected = Gson().fromJson(innerRecord, com.google.gson.JsonObject::class.java)
            expected.addProperty("@type", "MONGO_DELETE")
            actual shouldBe expected
            wasChanged shouldBe true
        }

        "fun testReformatArchivedOverwritesTypeAttribute()" {
            val mapUtility = MapUtility(mock())
            val innerRecord = """{ "_id": "123456789", "@type": "MONGO_INSERT" }"""
            val recordWithArchivedElement = """{ "_archived": $innerRecord }"""
            val json = Gson().fromJson(recordWithArchivedElement, com.google.gson.JsonObject::class.java)
            val (actual, wasChanged) = mapUtility.reformatArchived(Gson(), json)
            val expected = Gson().fromJson("""{ "_id": "123456789", "@type": "MONGO_DELETE" }""", com.google.gson.JsonObject::class.java)
            actual shouldBe expected
            wasChanged shouldBe true
        }

        "fun testReformatArchivedReturnsOuterRecordWhenArchivedElementDoesNotExist()" {
            val mapUtility = MapUtility(mock())
            val outerRecord = """{ "_id": "123456789" }""".trimIndent()
            val json = Gson().fromJson(outerRecord, com.google.gson.JsonObject::class.java)
            val (actual, wasChanged) = mapUtility.reformatArchived(Gson(), json)
            val expected = Gson().fromJson(outerRecord, com.google.gson.JsonObject::class.java)
            actual shouldBe expected
            wasChanged shouldBe false
        }

        "fun testReformatTransplantsLastModifiedWhenArchivedElementExists()" {
            val mapUtility = MapUtility(mock())
            val innerRecord = """{ "_id": "123456789" }""".trimIndent()
            val innerRecordWithTimestamp = """{ "_id": "123456789", "_lastModifiedDateTime": "OUTER_LAST_MODIFIED" }""".trimIndent()
            val recordWithArchivedElement = """{ "_archived": $innerRecord, "_lastModifiedDateTime": "OUTER_LAST_MODIFIED" }"""
            val json = Gson().fromJson(recordWithArchivedElement, com.google.gson.JsonObject::class.java)
            val (actual, wasChanged) = mapUtility.reformatArchived(Gson(), json)
            val expected = Gson().fromJson(innerRecordWithTimestamp, com.google.gson.JsonObject::class.java)
            expected.addProperty("@type", "MONGO_DELETE")
            actual shouldBe expected
            wasChanged shouldBe true
        }

        "fun testReformatReplacesLastModifiedWhenArchivedElementExists()" {
            val mapUtility = MapUtility(mock())
            val innerRecord = """{ "_id": "123456789", "_lastModifiedDateTime": "INNER_LAST_MODIFIED" }""".trimIndent()
            val innerRecordWithTimestamp = """{ "_id": "123456789", "_lastModifiedDateTime": "OUTER_LAST_MODIFIED" }""".trimIndent()
            val recordWithArchivedElement = """{ "_archived": $innerRecord, "_lastModifiedDateTime": "OUTER_LAST_MODIFIED" }"""
            val json = Gson().fromJson(recordWithArchivedElement, com.google.gson.JsonObject::class.java)
            val (actual, wasChanged) = mapUtility.reformatArchived(Gson(), json)
            val expected = Gson().fromJson(innerRecordWithTimestamp, com.google.gson.JsonObject::class.java)
            expected.addProperty("@type", "MONGO_DELETE")
            actual shouldBe expected
            wasChanged shouldBe true
        }

        "fun testReformatTransplantsTimestampWhenArchivedElementExists()" {
            val mapUtility = MapUtility(mock())
            val innerRecord = """{ "_id": "123456789" }""".trimIndent()
            val innerRecordWithTimestamp = """{ "_id": "123456789", "timestamp": "OUTER_TIMESTAMP" }""".trimIndent()
            val recordWithArchivedElement = """{ "_archived": $innerRecord, "timestamp": "OUTER_TIMESTAMP" }"""
            val json = Gson().fromJson(recordWithArchivedElement, com.google.gson.JsonObject::class.java)
            val (actual, wasChanged) = mapUtility.reformatArchived(Gson(), json)
            val expected = Gson().fromJson(innerRecordWithTimestamp, com.google.gson.JsonObject::class.java)
            expected.addProperty("@type", "MONGO_DELETE")
            actual shouldBe expected
            wasChanged shouldBe true
        }

        "fun testReformatReplacesTimestampWhenArchivedElementExists()" {
            val mapUtility = MapUtility(mock())
            val innerRecord = """{ "_id": "123456789", "timestamp": "INNER_TIMESTAMP" }""".trimIndent()
            val innerRecordWithTimestamp = """{ "_id": "123456789", "timestamp": "OUTER_TIMESTAMP" }""".trimIndent()
            val recordWithArchivedElement = """{ "_archived": $innerRecord, "timestamp": "OUTER_TIMESTAMP" }"""
            val json = Gson().fromJson(recordWithArchivedElement, com.google.gson.JsonObject::class.java)
            val (actual, wasChanged) = mapUtility.reformatArchived(Gson(), json)
            val expected = Gson().fromJson(innerRecordWithTimestamp, com.google.gson.JsonObject::class.java)
            expected.addProperty("@type", "MONGO_DELETE")
            actual shouldBe expected
            wasChanged shouldBe true
        }

        "fun testReformatTransplantsArchivedTimeWhenArchivedElementExists()" {
            val mapUtility = MapUtility(mock())
            val innerRecord = """{ "_id": "123456789" }""".trimIndent()
            val innerRecordWithTimestamp = """{ "_id": "123456789", "_archivedDateTime": "OUTER_ARCHIVED_TIME" }""".trimIndent()
            val recordWithArchivedElement = """{ "_archived": $innerRecord, "_archivedDateTime": "OUTER_ARCHIVED_TIME" }"""
            val json = Gson().fromJson(recordWithArchivedElement, com.google.gson.JsonObject::class.java)
            val (actual, wasChanged) = mapUtility.reformatArchived(Gson(), json)
            val expected = Gson().fromJson(innerRecordWithTimestamp, com.google.gson.JsonObject::class.java)
            expected.addProperty("@type", "MONGO_DELETE")
            actual shouldBe expected
            wasChanged shouldBe true
        }

        "fun testReformatReplacesArchivedTimeWhenArchivedElementExists()" {
            val mapUtility = MapUtility(mock())
            val innerRecord = """{ "_id": "123456789", "_archivedDateTime": "INNER_ARCHIVED_TIME" }""".trimIndent()
            val innerRecordWithTimestamp = """{ "_id": "123456789", "_archivedDateTime": "OUTER_ARCHIVED_TIME" }""".trimIndent()
            val recordWithArchivedElement = """{ "_archived": $innerRecord, "_archivedDateTime": "OUTER_ARCHIVED_TIME" }"""
            val json = Gson().fromJson(recordWithArchivedElement, com.google.gson.JsonObject::class.java)
            val (actual, wasChanged) = mapUtility.reformatArchived(Gson(), json)
            val expected = Gson().fromJson(innerRecordWithTimestamp, com.google.gson.JsonObject::class.java)
            expected.addProperty("@type", "MONGO_DELETE")
            actual shouldBe expected
            wasChanged shouldBe true
        }

    }
}
