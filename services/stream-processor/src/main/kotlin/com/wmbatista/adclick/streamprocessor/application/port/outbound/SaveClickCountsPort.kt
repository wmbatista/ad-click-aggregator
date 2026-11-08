package com.wmbatista.adclick.streamprocessor.application.port.outbound

import com.wmbatista.adclick.streamprocessor.domain.ClickCount

/** Stores per-minute click counts, adding them to any counts already stored for the same ad and minute. */
fun interface SaveClickCountsPort {
    /** Stores all [counts] at once, or throws if they could not be stored. */
    fun save(counts: List<ClickCount>)
}
