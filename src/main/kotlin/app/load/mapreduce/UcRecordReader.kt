package app.load.mapreduce

import app.load.domain.EncryptionMetadata
import app.load.services.impl.AESCipherService
import app.load.services.impl.RetryServiceImpl
import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.IOUtils
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.InputSplit
import org.apache.hadoop.mapreduce.RecordReader
import org.apache.hadoop.mapreduce.TaskAttemptContext
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import uk.gov.dwp.dataworks.logging.DataworksLogger
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.InputStreamReader
import java.io.LineNumberReader
import java.security.Key
import java.util.*
import javax.crypto.spec.SecretKeySpec

class UcRecordReader : RecordReader<LongWritable, Text>() {

    override fun initialize(split: InputSplit, context: TaskAttemptContext) =
            (split as FileSplit).path.let { path ->
                val retryService = RetryServiceImpl.connect()
                val cipherService = AESCipherService.connect()
                logger.info("Starting split", "path" to path.toString())
                path.getFileSystem(context.configuration).let { fs ->

                    filenameRegex.find(path.toString())?.let {
                        val (matchedDatabase, matchedCollection) = it.destructured
                        database = matchedDatabase
                        collection = coalesced(matchedCollection)
                        val originalTableName = "$database:$collection".replace("-", "_")
                        val tableName = coalescedArchive(originalTableName)
                        if (originalTableName != tableName) {
                            collection = tableName.replace(Regex("""^[^:]+:"""), "")
                        }
                    }

                    val metadataPath = Path(path.toString().replace("gz.enc", "encryption.json"))
                    val metadata = ObjectMapper().readValue(fs.open(metadataPath), EncryptionMetadata::class.java)
                    val plaintextKey = retryService.decryptKey(metadata.keyEncryptionKeyId, metadata.encryptedEncryptionKey)
                    val key: Key = SecretKeySpec(Base64.getDecoder().decode(plaintextKey), "AES")
                    val inputStream = byteArrayInputStream(fs, path)
                    input = LineNumberReader(InputStreamReader(cipherService.decompressingDecryptingStream(fs.open(path), key, metadata.initialisationVector)))
                    currentFileSystem = fs
                    currentPath = path
                }
            }

    private fun byteArrayInputStream(fs: FileSystem, path: Path?): ByteArrayInputStream =
            with (ByteArrayOutputStream()) {
                fs.open(path).use { it.copyTo(this) }
                ByteArrayInputStream(toByteArray())
            }

    override fun nextKeyValue() = hasNext(input?.readLine())

    override fun close() {
        IOUtils.closeStream(input)
        logger.info("Completed split", "path" to "${currentPath?.toString()}")
    }

    override fun getCurrentKey(): LongWritable = LongWritable()
    override fun getCurrentValue(): Text? = value
    override fun getProgress(): Float = .5f

    private fun hasNext(line: String?) =
            if (line != null) {
                value = Text(line)
                true
            } else {
                false
            }

    private var input: LineNumberReader? = null
    private var value: Text? = null
    private var currentPath: Path? = null
    private var currentFileSystem: FileSystem? = null

    fun coalesced(collection: String): String {
        val coalescedName = COALESCED_COLLECTION.replace(collection, "")
        if (collection != coalescedName) {
            logger.info("Using coalesced collection", "original_name" to collection, "coalesced_name" to coalescedName)
        }
        return coalescedName
    }

    fun coalescedArchive(tableName: String) =
            if (coalescedNames[tableName] != null) coalescedNames[tableName] ?: "" else tableName

    private val coalescedNames = mapOf("agent_core:agentToDoArchive" to "agent_core:agentToDo")

    companion object {
        private val logger = DataworksLogger.getLogger(UcRecordReader::class.java.toString())
        var database = ""
        var collection = ""
        private const val filenamePattern = """(?<database>[\w-]+)\.(?<collection>[\w-]+)\.(?<filenumber>[0-9]+)\.json\.gz\.enc$"""
        private val filenameRegex = Regex(filenamePattern, RegexOption.IGNORE_CASE)
        private val COALESCED_COLLECTION = Regex("-(archived|eight|eighteen|eleven|fifteen|five|four|fourteen|nine|nineteen|one|seven|seventeen|six|sixteen|ten|thirteen|thirty|thirtyone|thirtytwo|three|twelve|twenty|twentyeight|twentyfive|twentyfour|twentynine|twentyone|twentyseven|twentysix|twentythree|twentytwo|two)$")
    }
}
