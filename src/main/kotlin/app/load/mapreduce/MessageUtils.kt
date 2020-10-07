package app.load.mapreduce

import com.beust.klaxon.JsonObject
import com.beust.klaxon.Parser
import com.google.gson.Gson
import uk.gov.dwp.dataworks.logging.DataworksLogger
import java.nio.ByteBuffer
import java.text.ParseException
import java.text.SimpleDateFormat
import java.util.zip.CRC32

class MessageUtils {
    val logger = DataworksLogger.getLogger(MessageUtils::class.java.toString())

    fun parseGson(line: String): com.google.gson.JsonObject =
        Gson().fromJson(line, com.google.gson.JsonObject::class.java)

    @Throws(ParseException::class)
    fun getTimestampAsLong(timeStampAsStr: String?): Long {
        val validTimestamps = listOf("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", "yyyy-MM-dd'T'HH:mm:ss.SSSZZZZ")

        validTimestamps.forEach {
            try {
                val df = SimpleDateFormat(it)
                return df.parse(timeStampAsStr).time
            }
            catch (e: Exception) {

            }
        }
        throw ParseException("Unparseable date: '$timeStampAsStr'", 0)
    }

    fun generateKeyFromRecordBody(body: JsonObject?): ByteArray {
        val id: JsonObject? = body?.let { getId(it) }
        return if (id == null) ByteArray(0) else generateKey(id)
    }

    private fun getId(json: JsonObject): JsonObject? {
        val message = json["message"]
        if (message != null && message is JsonObject) {
            val id = message["_id"]

            if (id != null) {
                when (id) {
                    is JsonObject -> {
                        return id
                    }
                    is String -> {
                        val idObject = JsonObject()
                        idObject["id"] = id
                        return idObject
                    }
                    is Int -> {
                        val idObject = JsonObject()
                        idObject["id"] = "$id"
                        return idObject
                    }
                    else -> {
                        return null
                    }
                }
            }
            else {
                return null
            }

        }
        else {
            return null
        }
    }

    fun parseJson(line: String?): JsonObject {
        val parser: Parser = Parser.default()
        val stringBuilder = StringBuilder(line)
        return parser.parse(stringBuilder) as JsonObject
    }

    private fun generateKey(json: JsonObject): ByteArray {
        val jsonOrdered = sortJsonByKey(json)
        val checksumBytes: ByteArray = generateFourByteChecksum(jsonOrdered)

        return checksumBytes.plus(jsonOrdered.toByteArray())
    }

    private fun sortJsonByKey(unsortedJson: JsonObject): String {
        val sortedEntries = unsortedJson.toSortedMap(compareBy { it })
        val json = JsonObject(sortedEntries)
        return json.toJsonString()
    }

    private fun generateFourByteChecksum(input: String): ByteArray {
        val bytes = input.toByteArray()
        val checksum = CRC32()

        checksum.update(bytes, 0, bytes.size)

        return ByteBuffer.allocate(4).putInt(checksum.value.toInt()).array()
    }
}
