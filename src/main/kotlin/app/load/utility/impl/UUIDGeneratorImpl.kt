package app.load.utility.impl

import app.load.utility.UUIDGenerator
import java.util.*

class UUIDGeneratorImpl : UUIDGenerator {

    override fun randomUUID(): String {
        return UUID.randomUUID().toString()
    }
}
