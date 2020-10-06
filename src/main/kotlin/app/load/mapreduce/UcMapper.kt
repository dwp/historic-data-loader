package app.load.mapreduce

import app.load.services.impl.AESCipherService
import app.load.services.impl.HttpKeyService
import com.google.gson.GsonBuilder
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper
import uk.gov.dwp.dataworks.logging.DataworksLogger

class UcMapper: Mapper<LongWritable, Text, ImmutableBytesWritable, KeyValue>() {

    public override fun map(key: LongWritable, value: Text, context: Context) {
        try {
            val validBytes = bytes(value)
            val (id, message, version) = mapUtility.mappedRecord(gson, dataKeyResult, String(validBytes))
            val hkey = ImmutableBytesWritable().apply { set(id) }
            context.write(hkey, keyValue(hkey, version, Bytes.toBytes(message)))
        } catch (e: Exception) {
            logger.error("Failed to map record", e, "error" to "${e.message}")
        }
    }

    private fun keyValue(key: ImmutableBytesWritable, version: Long, bytes: ByteArray): KeyValue =
        KeyValue(key.get(), columnFamily, columnQualifier, version, bytes)

    private fun bytes(value: Text): ByteArray = value.bytes.sliceArray(0 until value.length)

    private val dataKeyResult by lazy {
        HttpKeyService.connect().batchDataKey()
    }

    companion object {
        private val logger = DataworksLogger.getLogger(UcMapper::class.java.toString())
        private val mapUtility = MapUtility(AESCipherService.connect())
        private val gson = GsonBuilder().serializeNulls().create()
        private val columnFamily by lazy { Bytes.toBytes("cf") }
        private val columnQualifier by lazy { Bytes.toBytes("record") }
    }
}
