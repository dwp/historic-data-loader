package app.load.mapreduce

import app.load.domain.DataKeyResult
import app.load.services.FilterService
import app.load.services.impl.AESCipherService
import app.load.services.impl.FilterServiceImpl
import com.google.gson.GsonBuilder
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper
import uk.gov.dwp.dataworks.logging.DataworksLogger

class UcMapper(): Mapper<LongWritable, Text, ImmutableBytesWritable, KeyValue>() {

    constructor(utility: MapUtility, retrier: RetryUtility): this() {
        mapUtility = utility
        retryUtility = retrier
    }

    public override fun map(key: LongWritable, value: Text, context: Context) {
        try {
            val validBytes = bytes(value)
            val (id, message, version, filterStatus) = mapUtility.mappedRecord(gson, dataKeyResult, String(validBytes))
            if (filterStatus == FilterService.FilterStatus.DoNotFilter) {
                val hkey = ImmutableBytesWritable().apply { set(id) }
                context.write(hkey, keyValue(hkey, version, Bytes.toBytes(message)))
            }
            else {
                logger.debug("Filtering record", "id" to "${String(id)}", "version" to "$version",
                        "status" to "$filterStatus")
            }
        } catch (e: Exception) {
            e.printStackTrace()
            logger.error("Failed to map record", e, "error" to "${e.message}")
        }
    }

    private fun keyValue(key: ImmutableBytesWritable, version: Long, bytes: ByteArray): KeyValue =
        KeyValue(key.get(), columnFamily, columnQualifier, version, bytes)

    private fun bytes(value: Text): ByteArray = value.bytes.sliceArray(0 until value.length)

    private val dataKeyResult: DataKeyResult by lazy {
        retryUtility.batchDataKey()
    }

    companion object {
        private val logger = DataworksLogger.getLogger(UcMapper::class.java.toString())
        private var mapUtility: MapUtility = MapUtilityImpl(AESCipherService.connect(), FilterServiceImpl.connect())
        private var retryUtility: RetryUtility = RetryUtilityImpl.connect()
        private val gson = GsonBuilder().serializeNulls().create()
        private val columnFamily by lazy { Bytes.toBytes("cf") }
        private val columnQualifier by lazy { Bytes.toBytes("record") }
    }
}
