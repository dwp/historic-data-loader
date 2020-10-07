package app.load.mapreduce

import com.google.gson.Gson
import com.nhaarman.mockitokotlin2.mock
import io.kotest.core.spec.style.StringSpec
import io.kotest.matchers.shouldBe

class ReformattingTests: StringSpec() {
    init {
        "testReformatNonRemovedReturnsUnmodifiedRecordWhenNoRemovedElementExists" {
            val mapUtility = MapUtilityImpl(mock(), mock())
            val innerRecord = """{ "_id": "123456789" }""".trimIndent()
            val recordWithRemovedElement = """{ "_notRemoved": $innerRecord }"""
            val (actual, wasChanged) = mapUtility.reformatRemoved(Gson(), recordWithRemovedElement)
            val expected = Gson().fromJson(recordWithRemovedElement, com.google.gson.JsonObject::class.java)
            actual shouldBe expected 
            wasChanged shouldBe false
        }

        "testReformatRemovedReturnsInnerRecordWhenRemovedElementExists" {
            val mapUtility = MapUtilityImpl(mock(), mock())
            val innerRecord = """{ "_id": "123456789" }""".trimIndent()
            val recordWithRemovedElement = """{ "_removed": $innerRecord }"""
            val (actual, wasChanged) = mapUtility.reformatRemoved(Gson(), recordWithRemovedElement)
            val expected = Gson().fromJson(innerRecord, com.google.gson.JsonObject::class.java)
            expected.addProperty("@type", "MONGO_DELETE")
            actual shouldBe expected
            wasChanged shouldBe true
        }

        "testReformatRemovedOverwritesTypeAttribute" {
            val mapUtility = MapUtilityImpl(mock(), mock())
            val innerRecord = """{ "_id": "123456789", "@type": "MONGO_INSERT" }"""
            val recordWithRemovedElement = """{ "_removed": $innerRecord }"""
            val (actual, wasChanged) = mapUtility.reformatRemoved(Gson(), recordWithRemovedElement)
            val expected = Gson().fromJson("""{ "_id": "123456789", "@type": "MONGO_DELETE" }""", com.google.gson.JsonObject::class.java)
            actual shouldBe expected
            wasChanged shouldBe true
        }

        "testReformatRemovedReturnsOuterRecordWhenRemovedElementDoesNotExist" {
            val mapUtility = MapUtilityImpl(mock(), mock())
            val outerRecord = """{ "_id": "123456789" }""".trimIndent()
            val (actual, wasChanged) = mapUtility.reformatRemoved(Gson(), outerRecord)
            val expected = Gson().fromJson(outerRecord, com.google.gson.JsonObject::class.java)
            actual shouldBe expected
            wasChanged shouldBe false
        }

        "testReformatTransplantsLastModifiedWhenRemovedElementExists" {
            val mapUtility = MapUtilityImpl(mock(), mock())
            val innerRecord = """{ "_id": "123456789" }""".trimIndent()
            val innerRecordWithTimestamp = """{ "_id": "123456789", "_lastModifiedDateTime": "OUTER_LAST_MODIFIED" }""".trimIndent()
            val recordWithRemovedElement = """{ "_removed": $innerRecord, "_lastModifiedDateTime": "OUTER_LAST_MODIFIED" }"""
            val (actual, wasChanged) = mapUtility.reformatRemoved(Gson(), recordWithRemovedElement)
            val expected = Gson().fromJson(innerRecordWithTimestamp, com.google.gson.JsonObject::class.java)
            expected.addProperty("@type", "MONGO_DELETE")
            actual shouldBe expected
            wasChanged shouldBe true
        }

        "testReformatReplacesLastModifiedWhenRemovedElementExists" {
            val mapUtility = MapUtilityImpl(mock(), mock())
            val innerRecord = """{ "_id": "123456789", "_lastModifiedDateTime": "INNER_LAST_MODIFIED" }""".trimIndent()
            val innerRecordWithTimestamp = """{ "_id": "123456789", "_lastModifiedDateTime": "OUTER_LAST_MODIFIED" }""".trimIndent()
            val recordWithRemovedElement = """{ "_removed": $innerRecord, "_lastModifiedDateTime": "OUTER_LAST_MODIFIED" }"""
            val (actual, wasChanged) = mapUtility.reformatRemoved(Gson(), recordWithRemovedElement)
            val expected = Gson().fromJson(innerRecordWithTimestamp, com.google.gson.JsonObject::class.java)
            expected.addProperty("@type", "MONGO_DELETE")
            actual shouldBe expected
            wasChanged shouldBe true
        }

        "testReformatTransplantsRemovedTimeWhenRemovedElementExists" {
            val mapUtility = MapUtilityImpl(mock(), mock())
            val innerRecord = """{ "_id": "123456789" }""".trimIndent()
            val innerRecordWithTimestamp = """{ "_id": "123456789", "_removedDateTime": "OUTER_REMOVED_TIME" }""".trimIndent()
            val recordWithRemovedElement = """{ "_removed": $innerRecord, "_removedDateTime": "OUTER_REMOVED_TIME" }"""
            val (actual, wasChanged) = mapUtility.reformatRemoved(Gson(), recordWithRemovedElement)
            val expected = Gson().fromJson(innerRecordWithTimestamp, com.google.gson.JsonObject::class.java)
            expected.addProperty("@type", "MONGO_DELETE")
            actual shouldBe expected
            wasChanged shouldBe true
        }

        "testReformatReplacesRemovedTimeWhenRemovedElementExists" {
            val mapUtility = MapUtilityImpl(mock(), mock())
            val innerRecord = """{ "_id": "123456789", "_removedDateTime": "INNER_REMOVED_TIME" }""".trimIndent()
            val innerRecordWithTimestamp = """{ "_id": "123456789", "_removedDateTime": "OUTER_REMOVED_TIME" }""".trimIndent()
            val recordWithRemovedElement = """{ "_removed": $innerRecord, "_removedDateTime": "OUTER_REMOVED_TIME" }"""
            val (actual, wasChanged) = mapUtility.reformatRemoved(Gson(), recordWithRemovedElement)
            val expected = Gson().fromJson(innerRecordWithTimestamp, com.google.gson.JsonObject::class.java)
            expected.addProperty("@type", "MONGO_DELETE")
            actual shouldBe expected
            wasChanged shouldBe true
        }

        "testReformatTransplantsTimestampWhenRemovedElementExists" {
            val mapUtility = MapUtilityImpl(mock(), mock())
            val innerRecord = """{ "_id": "123456789" }""".trimIndent()
            val innerRecordWithTimestamp = """{ "_id": "123456789", "timestamp": "OUTER_TIMESTAMP" }""".trimIndent()
            val recordWithRemovedElement = """{ "_removed": $innerRecord, "timestamp": "OUTER_TIMESTAMP" }"""
            val (actual, wasChanged) = mapUtility.reformatRemoved(Gson(), recordWithRemovedElement)
            val expected = Gson().fromJson(innerRecordWithTimestamp, com.google.gson.JsonObject::class.java)
            expected.addProperty("@type", "MONGO_DELETE")
            actual shouldBe expected
            wasChanged shouldBe true
        }

        "testReformatReplacesTimestampWhenRemovedElementExists" {
            val mapUtility = MapUtilityImpl(mock(), mock())
            val innerRecord = """{ "_id": "123456789", "timestamp": "INNER_TIMESTAMP" }""".trimIndent()
            val innerRecordWithTimestamp = """{ "_id": "123456789", "timestamp": "OUTER_TIMESTAMP" }""".trimIndent()
            val recordWithRemovedElement = """{ "_removed": $innerRecord, "timestamp": "OUTER_TIMESTAMP" }"""
            val (actual, wasChanged) = mapUtility.reformatRemoved(Gson(), recordWithRemovedElement)
            val expected = Gson().fromJson(innerRecordWithTimestamp, com.google.gson.JsonObject::class.java)
            expected.addProperty("@type", "MONGO_DELETE")
            actual shouldBe expected
            wasChanged shouldBe true
        }

        "testReformatNonArchivedReturnsUnmodifiedRecordWhenNoArchivedElementExists" {
            val mapUtility = MapUtilityImpl(mock(), mock())
            val innerRecord = """{ "_id": "123456789" }""".trimIndent()
            val recordWithArchivedElement = """{ "_notArchived": $innerRecord }"""
            val json = Gson().fromJson(recordWithArchivedElement, com.google.gson.JsonObject::class.java)
            val (actual, wasChanged) = mapUtility.reformatArchived(Gson(), json)
            val expected = Gson().fromJson(recordWithArchivedElement, com.google.gson.JsonObject::class.java)
            actual shouldBe expected
            wasChanged shouldBe false
        }

        "testReformatArchivedReturnsInnerRecordWhenArchivedElementExists" {
            val mapUtility = MapUtilityImpl(mock(), mock())
            val innerRecord = """{ "_id": "123456789" }""".trimIndent()
            val recordWithArchivedElement = """{ "_archived": $innerRecord }"""
            val json = Gson().fromJson(recordWithArchivedElement, com.google.gson.JsonObject::class.java)
            val (actual, wasChanged) = mapUtility.reformatArchived(Gson(), json)
            val expected = Gson().fromJson(innerRecord, com.google.gson.JsonObject::class.java)
            expected.addProperty("@type", "MONGO_DELETE")
            actual shouldBe expected
            wasChanged shouldBe true
        }

        "testReformatArchivedOverwritesTypeAttribute" {
            val mapUtility = MapUtilityImpl(mock(), mock())
            val innerRecord = """{ "_id": "123456789", "@type": "MONGO_INSERT" }"""
            val recordWithArchivedElement = """{ "_archived": $innerRecord }"""
            val json = Gson().fromJson(recordWithArchivedElement, com.google.gson.JsonObject::class.java)
            val (actual, wasChanged) = mapUtility.reformatArchived(Gson(), json)
            val expected = Gson().fromJson("""{ "_id": "123456789", "@type": "MONGO_DELETE" }""", com.google.gson.JsonObject::class.java)
            actual shouldBe expected
            wasChanged shouldBe true
        }

        "testReformatArchivedReturnsOuterRecordWhenArchivedElementDoesNotExist" {
            val mapUtility = MapUtilityImpl(mock(), mock())
            val outerRecord = """{ "_id": "123456789" }""".trimIndent()
            val json = Gson().fromJson(outerRecord, com.google.gson.JsonObject::class.java)
            val (actual, wasChanged) = mapUtility.reformatArchived(Gson(), json)
            val expected = Gson().fromJson(outerRecord, com.google.gson.JsonObject::class.java)
            actual shouldBe expected
            wasChanged shouldBe false
        }

        "testReformatTransplantsLastModifiedWhenArchivedElementExists" {
            val mapUtility = MapUtilityImpl(mock(), mock())
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

        "testReformatReplacesLastModifiedWhenArchivedElementExists" {
            val mapUtility = MapUtilityImpl(mock(), mock())
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

        "testReformatTransplantsTimestampWhenArchivedElementExists" {
            val mapUtility = MapUtilityImpl(mock(), mock())
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

        "testReformatReplacesTimestampWhenArchivedElementExists" {
            val mapUtility = MapUtilityImpl(mock(), mock())
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

        "testReformatTransplantsArchivedTimeWhenArchivedElementExists" {
            val mapUtility = MapUtilityImpl(mock(), mock())
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

        "testReformatReplacesArchivedTimeWhenArchivedElementExists" {
            val mapUtility = MapUtilityImpl(mock(), mock())
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
