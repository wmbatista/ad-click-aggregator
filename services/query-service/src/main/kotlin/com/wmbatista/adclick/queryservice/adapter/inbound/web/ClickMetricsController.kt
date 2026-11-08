package com.wmbatista.adclick.queryservice.adapter.inbound.web

import com.wmbatista.adclick.queryservice.application.port.inbound.GetClickMetricsUseCase
import com.wmbatista.adclick.queryservice.domain.ClickQuery
import com.wmbatista.adclick.queryservice.domain.Granularity
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import java.time.Instant

@RestController
class ClickMetricsController(
    private val getClickMetricsUseCase: GetClickMetricsUseCase,
) {

    /** Clicks on [adId] over `[from, to)`, one zero-filled bucket per [granularity] step. */
    @GetMapping("/ads/{adId}/clicks")
    fun clicks(
        @PathVariable adId: String,
        @RequestParam from: Instant,
        @RequestParam to: Instant,
        @RequestParam(defaultValue = "MINUTE") granularity: Granularity,
    ): ClickMetricsResponse {
        val query = ClickQuery.of(adId, from, to, granularity)
        return ClickMetricsResponse.from(getClickMetricsUseCase.getClickMetrics(query))
    }
}
