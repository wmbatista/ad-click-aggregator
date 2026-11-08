package com.wmbatista.adclick.clickservice.application.port.outbound

import java.util.UUID

/** Generates the unique identifier assigned to each registered click. */
fun interface ClickIdGenerator {
    fun next(): UUID
}
