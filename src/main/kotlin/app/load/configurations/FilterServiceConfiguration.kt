package app.load.configurations

object FilterServiceConfiguration {
    val propertiesFile = System.getenv("FILTER_SERVICE_PROPERTIES") ?: "/opt/emr/filter.properties"
    val earlierThan = System.getenv("FILTER_SERVICE_EARLIER_THAN") ?: ""
    val laterThan = System.getenv("FILTER_SERVICE_EARLIER_LATER") ?: ""
}
