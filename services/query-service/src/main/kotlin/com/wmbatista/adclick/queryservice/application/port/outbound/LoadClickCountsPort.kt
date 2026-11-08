package com.wmbatista.adclick.queryservice.application.port.outbound

import com.wmbatista.adclick.queryservice.domain.ClickQuery
import java.time.Instant

/** Reads aggregated click counts from the click store. */
interface LoadClickCountsPort {
    /**
     * Returns the click count per bucket start for [query]; buckets without clicks are absent.
     *
     * @throws ClickStoreUnavailableException if the store cannot be read.
     */
    fun countsPerBucket(query: ClickQuery): Map<Instant, Long>
}
