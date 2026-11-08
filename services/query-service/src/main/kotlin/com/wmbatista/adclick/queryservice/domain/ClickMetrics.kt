package com.wmbatista.adclick.queryservice.domain

import java.time.Instant

/** Clicks in the bucket that starts at [start]. */
data class ClickBucket(val start: Instant, val count: Long)

/** Clicks on [adId] over `[from, to)`, one bucket per [granularity] step, zero-filled and in order. */
data class ClickMetrics(
    val adId: String,
    val granularity: Granularity,
    val from: Instant,
    val to: Instant,
    val buckets: List<ClickBucket>,
) {
    val total: Long get() = buckets.sumOf { it.count }
}
