package app.load.mapreduce

import app.load.domain.EncryptionMetadata
import app.load.services.impl.AESCipherService
import app.load.services.impl.HttpKeyService
import app.load.utility.Converter
import app.load.utility.MessageParser
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
import java.io.BufferedReader
import java.io.InputStreamReader
import java.io.LineNumberReader
import java.security.Key
import java.util.*
import java.util.zip.GZIPInputStream
import javax.crypto.spec.SecretKeySpec
import kotlin.time.ExperimentalTime

class UcRecordReader : RecordReader<LongWritable, Text>() {

    override fun initialize(split: InputSplit, context: TaskAttemptContext) =
            (split as FileSplit).path.let { path ->
                val keyService = HttpKeyService.connect()
                val cipherService = AESCipherService.connect()
                logger.info("Starting split", "path" to path.toString())
                path.getFileSystem(context.configuration).let { fs ->

                    filenameRegex.find(path.toString())?.let {
                        val (matchedDatabase, matchedCollection) = it.destructured
                        database = matchedDatabase
                        collection = matchedCollection
                    }

                    val metadataPath = Path(path.toString().replace("gz.enc", "encryption.json"))
                    val metadata = ObjectMapper().readValue(fs.open(metadataPath), EncryptionMetadata::class.java)
                    val plaintextKey = keyService.decryptKey(metadata.keyEncryptionKeyId, metadata.encryptedEncryptionKey)
                    val key: Key = SecretKeySpec(Base64.getDecoder().decode(plaintextKey), "AES")

                    input = LineNumberReader(InputStreamReader(cipherService.decompressingDecryptingStream(fs.open(path), key, metadata.initialisationVector)))
                    currentFileSystem = fs
                    currentPath = path
                }
            }

    override fun nextKeyValue() = hasNext(input?.readLine())

    @ExperimentalTime
    override fun close() {
        IOUtils.closeStream(input)
        logger.info("Completed split", "path" to "${currentPath?.toString()}")
    }

    override fun getCurrentKey(): LongWritable = LongWritable()
    override fun getCurrentValue(): Text? = value
    override fun getProgress(): Float = .5f

    private fun hasNext(line: String?) =
            if (line != null) {
                println("LINE: '$line'.")
                value = Text(line)
                true
            } else {
                false
            }

    private var input: LineNumberReader? = null
    private var value: Text? = null
    private var currentPath: Path? = null
    private var currentFileSystem: FileSystem? = null

    companion object {
        private val logger = DataworksLogger.getLogger(UcRecordReader::class.java.toString())
        var database = "UNSET"
        var collection = "UNSET"
        private val filenamePattern = """(?<database>[\w-]+)\.(?<collection>[[\w-]+]+)\.(?<filenumber>[0-9]+)\.json\.gz\.enc$"""
        private val filenameRegex = Regex(filenamePattern, RegexOption.IGNORE_CASE)
    }
}
