package app.load.mapreduce

import app.load.utility.Converter
import app.load.utility.MessageParser
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
import java.util.zip.GZIPInputStream
import kotlin.time.ExperimentalTime

class UcRecordReader : RecordReader<LongWritable, Text>() {

    override fun initialize(split: InputSplit, context: TaskAttemptContext) =
            (split as FileSplit).path.let { path ->
                logger.info("Starting split", "path" to path.toString())
                path.getFileSystem(context.configuration).let { fs ->
                    input = BufferedReader(InputStreamReader(GZIPInputStream(fs.open(path))))
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
                value = Text(line)
                true
            } else {
                false
            }

    private var input: BufferedReader? = null
    private var value: Text? = null
    private var currentPath: Path? = null
    private var currentFileSystem: FileSystem? = null
    private val messageParser = MessageParser()
    private val convertor = Converter()

    companion object {
        private val logger = DataworksLogger.getLogger(UcRecordReader::class.java.toString())
    }
}
