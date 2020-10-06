package app.load.services.impl

import app.load.services.FilterService
import org.apache.commons.lang3.StringUtils
import uk.gov.dwp.dataworks.logging.DataworksLogger
import java.text.SimpleDateFormat

class FilterServiceImpl : FilterService {

    override fun filterStatus(timestamp: Long): FilterService.FilterStatus =
            when {
                // timestamp == epoch means a record with no last modified date
                // so put these in as a precaution as they may be recent.
                timestamp < earlierThan && timestamp != epoch -> {
                    FilterService.FilterStatus.FilterTooEarly
                }
                timestamp > laterThan -> {
                    FilterService.FilterStatus.FilterTooLate
                }
                else -> {
                    FilterService.FilterStatus.DoNotFilter
                }
            }

    private val earlierThan: Long by lazy {
        if (StringUtils.isNotBlank(skipEarlierThan)) {
            if (alternateDateFormatPattern.matches(skipEarlierThan)) {
                SimpleDateFormat(alternateDateFormat).parse(skipEarlierThan).time
            }
            else {
                SimpleDateFormat(dateFormat).parse(skipEarlierThan).time
            }
        }
        else {
            Long.MIN_VALUE
        }
    }

    private val laterThan: Long by lazy {
        if (StringUtils.isNotBlank(skipLaterThan)) {
            if (alternateDateFormatPattern.matches(skipLaterThan)) {
                SimpleDateFormat(alternateDateFormat).parse(skipLaterThan).time
            }
            else {
                SimpleDateFormat(dateFormat).parse(skipLaterThan).time
            }
        }
        else {
            Long.MAX_VALUE
        }
    }

    private var skipEarlierThan: String = ""

    private var skipLaterThan: String = ""

    companion object {
        const val dateFormat = "yyyy-MM-dd'T'HH:mm:ss.SSS"
        const val alternateDateFormat = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
        val epoch = SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZZZZ").parse("1980-01-01T00:00:00.000+0000").time
        val alternateDateFormatPattern = Regex("""Z$""")
        val logger = DataworksLogger.getLogger(FilterServiceImpl::class.java.toString())
    }
}
