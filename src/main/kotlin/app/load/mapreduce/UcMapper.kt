package app.load.mapreduce

import app.load.services.impl.HttpKeyService
import app.load.utility.Converter
import app.load.utility.MessageParser
import com.beust.klaxon.JsonObject
import com.google.gson.Gson
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper

class UcMapper: Mapper<LongWritable, Text, ImmutableBytesWritable, KeyValue>() {

    public override fun map(key: LongWritable, value: Text, context: Context) {
        val validBytes = bytes(value)
        val (id, message, version) = mapUtility.body(gson, dataKeyResult, String(validBytes))
        val hkey = ImmutableBytesWritable().apply { set(id) }
        context.write(hkey, keyValue(hkey, version, Bytes.toBytes(message)))
    }

    private fun keyValue(key: ImmutableBytesWritable, version: Long, bytes: ByteArray): KeyValue =
        KeyValue(key.get(), columnFamily, columnQualifier, version, bytes)

    private fun bytes(value: Text): ByteArray = value.bytes.sliceArray(0 until value.length)

    private val dataKeyResult by lazy {
        HttpKeyService.connect().batchDataKey()
    }

    companion object {
        private val mapUtility = MapUtility()
        private val gson = Gson()
        private val columnFamily by lazy { Bytes.toBytes("cf") }
        private val columnQualifier by lazy { Bytes.toBytes("record") }
    }
}
