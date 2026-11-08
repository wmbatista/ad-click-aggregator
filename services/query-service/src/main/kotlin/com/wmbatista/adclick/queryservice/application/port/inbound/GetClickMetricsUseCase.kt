package com.wmbatista.adclick.queryservice.application.port.inbound

import com.wmbatista.adclick.queryservice.domain.ClickMetrics
import com.wmbatista.adclick.queryservice.domain.ClickQuery

/** Answers how many clicks an ad received, bucketed over a time range. */
interface GetClickMetricsUseCase {
    /**
     * Returns one bucket per step of [query], including buckets without clicks.
     *
     * @throws com.wmbatista.adclick.queryservice.application.port.outbound.ClickStoreUnavailableException
     *   if the click store cannot be read.
     */
    fun getClickMetrics(query: ClickQuery): ClickMetrics
}
