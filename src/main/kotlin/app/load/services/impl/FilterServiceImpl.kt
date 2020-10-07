package app.load.services.impl

import app.load.configurations.FilterServiceConfiguration
import app.load.services.FilterService
import org.apache.commons.lang3.StringUtils
import uk.gov.dwp.dataworks.logging.DataworksLogger
import java.io.File
import java.io.FileReader
import java.text.SimpleDateFormat
import java.util.*

class FilterServiceImpl(private val propertiesFile: String,
                        private val skipEarlierThan: String,
                        private val skipLaterThan: String): FilterService {

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
        if (properties.getProperty("filter.earlier.than") != null) {
            parsedDate(properties.getProperty("filter.earlier.than"))
        }
        else if (StringUtils.isNotBlank(skipEarlierThan)) {
            parsedDate(skipEarlierThan)
        }
        else {
            Long.MIN_VALUE
        }
    }

    private val laterThan: Long by lazy {
        if (properties.getProperty("filter.later.than") != null) {
            parsedDate(properties.getProperty("filter.later.than"))
        }
        else if (StringUtils.isNotBlank(skipLaterThan)) {
            parsedDate(skipLaterThan)
        }
        else {
            Long.MAX_VALUE
        }
    }

    private fun parsedDate(date: String) =
            if (alternateDateFormatPattern.matches(date)) {
                SimpleDateFormat(alternateDateFormat).parse(date).time
            }
            else {
                SimpleDateFormat(dateFormat).parse(date).time
            }

    private val properties: Properties by lazy {
        Properties().apply {
            if (File(propertiesFile).isFile) {
                load(FileReader(propertiesFile))
            }
        }
    }

    companion object {
        const val dateFormat = "yyyy-MM-dd'T'HH:mm:ss.SSS"
        const val alternateDateFormat = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"
        val epoch = SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZZZZ").parse("1980-01-01T00:00:00.000+0000").time
        val alternateDateFormatPattern = Regex("""Z$""")
        val logger = DataworksLogger.getLogger(FilterServiceImpl::class.java.toString())

        fun connect() = FilterServiceImpl(FilterServiceConfiguration.propertiesFile,
                                                        FilterServiceConfiguration.earlierThan,
                                                        FilterServiceConfiguration.laterThan)
    }
}
