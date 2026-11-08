package com.wmbatista.adclick.queryservice.application.service

import com.wmbatista.adclick.queryservice.application.port.inbound.GetClickMetricsUseCase
import com.wmbatista.adclick.queryservice.application.port.outbound.LoadClickCountsPort
import com.wmbatista.adclick.queryservice.domain.ClickBucket
import com.wmbatista.adclick.queryservice.domain.ClickMetrics
import com.wmbatista.adclick.queryservice.domain.ClickQuery

class GetClickMetricsService(
    private val loadClickCounts: LoadClickCountsPort,
) : GetClickMetricsUseCase {

    /** The store only returns buckets that have clicks; every other bucket in the range is reported as zero. */
    override fun getClickMetrics(query: ClickQuery): ClickMetrics {
        val counts = loadClickCounts.countsPerBucket(query)
        val buckets = query.bucketStarts.map { start -> ClickBucket(start, counts[start] ?: 0) }
        return ClickMetrics(query.adId, query.granularity, query.from, query.to, buckets)
    }
}
