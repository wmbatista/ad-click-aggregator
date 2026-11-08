package com.wmbatista.adclick.streamprocessor.application.service

import com.wmbatista.adclick.streamprocessor.application.port.inbound.AggregateClicksUseCase
import com.wmbatista.adclick.streamprocessor.application.port.outbound.SaveClickCountsPort
import com.wmbatista.adclick.streamprocessor.domain.Click
import com.wmbatista.adclick.streamprocessor.domain.aggregatePerMinute

class AggregateClicksService(
    private val saveClickCountsPort: SaveClickCountsPort,
) : AggregateClicksUseCase {

    override fun aggregate(clicks: List<Click>) {
        val counts = aggregatePerMinute(clicks)
        if (counts.isNotEmpty()) {
            saveClickCountsPort.save(counts)
        }
    }
}
