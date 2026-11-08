package com.wmbatista.adclick.streamprocessor.application.port.inbound

import com.wmbatista.adclick.streamprocessor.domain.Click

/** Counts one batch of clicks per ad and minute and stores the counts. */
fun interface AggregateClicksUseCase {
    /**
     * Returns only after the counts are stored; throws if they could not be, in which case the
     * caller must not acknowledge the batch.
     */
    fun aggregate(clicks: List<Click>)
}
