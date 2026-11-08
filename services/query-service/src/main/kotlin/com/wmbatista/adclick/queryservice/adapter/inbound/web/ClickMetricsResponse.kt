package com.wmbatista.adclick.queryservice.adapter.inbound.web

import com.wmbatista.adclick.queryservice.domain.ClickMetrics
import com.wmbatista.adclick.queryservice.domain.Granularity
import java.time.Instant

/** Body of `GET /ads/{adId}/clicks`. [from] and [to] are the range after alignment to [granularity]. */
data class ClickMetricsResponse(
    val adId: String,
    val granularity: Granularity,
    val from: Instant,
    val to: Instant,
    val total: Long,
    val buckets: List<Bucket>,
) {
    data class Bucket(val start: Instant, val count: Long)

    companion object {
        fun from(metrics: ClickMetrics) = ClickMetricsResponse(
            adId = metrics.adId,
            granularity = metrics.granularity,
            from = metrics.from,
            to = metrics.to,
            total = metrics.total,
            buckets = metrics.buckets.map { Bucket(it.start, it.count) },
        )
    }
}
