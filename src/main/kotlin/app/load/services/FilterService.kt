package app.load.services

interface FilterService {
    fun filterStatus(timestamp: Long): FilterStatus
    enum class FilterStatus { DoNotFilter, FilterTooEarly, FilterTooLate }
}
