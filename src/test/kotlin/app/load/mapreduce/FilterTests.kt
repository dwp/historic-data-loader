package app.load.mapreduce

import app.load.domain.DataKeyResult
import app.load.domain.MappedRecord
import app.load.services.FilterService
import app.load.services.RetryService
import com.nhaarman.mockitokotlin2.*
import io.kotest.core.spec.style.StringSpec
import org.apache.hadoop.hbase.KeyValue
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.Mapper

class FilterTests: StringSpec()  {

    init {
        "shouldNotAddToBatchIfFilteredTooEarly" {
            val ucMapper = ucMapper(mappedRecord(FilterService.FilterStatus.FilterTooEarly))
            val context = context()
            ucMapper.map(mock(), text(), context)
            verifyZeroInteractions(context)
        }

        "shouldNotAddToBatchIfFilteredTooLate" {
            val ucMapper = ucMapper(mappedRecord(FilterService.FilterStatus.FilterTooLate))
            val context = context()
            ucMapper.map(mock(), text(), context)
            verifyZeroInteractions(context)
        }

        "shouldAddToBatchIfNotFiltered" {
            val ucMapper = ucMapper(mappedRecord(FilterService.FilterStatus.DoNotFilter))
            val context = context()
            ucMapper.map(mock(), text(), context)
            verify(context, times(1)).write(any(), any())
        }
    }
    private fun mappedRecord(filterStatus: FilterService.FilterStatus)
            = MappedRecord(ByteArray(0), "", 0L, filterStatus)

    private fun text() =
        mock<Text> {
            on { bytes } doReturn "123456789XXX".toByteArray()
            on { length } doReturn 9
        }

    private fun context() =
            mock<Mapper<LongWritable, Text, ImmutableBytesWritable, KeyValue>.Context>()

    private fun ucMapper(mappedRecord: MappedRecord): UcMapper {
        val mapUtility = mock<MapUtility> {
            on { mappedRecord(any(), any(), any())} doReturn mappedRecord
        }

        val retryUtility = mock<RetryService> {
            on { batchDataKey() } doReturn DataKeyResult("dataKeyEncryptionKeyId", "plaintextDataKey", "ciphertextDataKey")
        }

        return UcMapper(mapUtility, retryUtility)
    }

}
