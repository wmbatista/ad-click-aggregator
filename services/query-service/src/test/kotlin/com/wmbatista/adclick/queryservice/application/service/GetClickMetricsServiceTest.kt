package com.wmbatista.adclick.queryservice.application.service

import com.wmbatista.adclick.queryservice.application.port.outbound.ClickStoreUnavailableException
import com.wmbatista.adclick.queryservice.application.port.outbound.LoadClickCountsPort
import com.wmbatista.adclick.queryservice.domain.ClickBucket
import com.wmbatista.adclick.queryservice.domain.ClickQuery
import com.wmbatista.adclick.queryservice.domain.Granularity.MINUTE
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.time.Instant

class GetClickMetricsServiceTest {

    private fun t(s: String) = Instant.parse(s)

    private val port = mockk<LoadClickCountsPort>()
    private val service = GetClickMetricsService(port)
    private val query = ClickQuery.of("ad-123", t("2026-11-08T10:00:00Z"), t("2026-11-08T10:03:00Z"), MINUTE)

    @Test
    fun `zero-fills missing buckets in order`() {
        every { port.countsPerBucket(query) } returns
            mapOf(t("2026-11-08T10:02:00Z") to 2L, t("2026-11-08T10:00:00Z") to 5L)

        val metrics = service.getClickMetrics(query)

        assertEquals(
            listOf(
                ClickBucket(t("2026-11-08T10:00:00Z"), 5),
                ClickBucket(t("2026-11-08T10:01:00Z"), 0),
                ClickBucket(t("2026-11-08T10:02:00Z"), 2),
            ),
            metrics.buckets,
        )
        assertEquals(7, metrics.total)
        assertEquals("ad-123", metrics.adId)
        assertEquals(MINUTE, metrics.granularity)
        assertEquals(query.from, metrics.from)
        assertEquals(query.to, metrics.to)
    }

    @Test
    fun `no rows gives all zeros`() {
        every { port.countsPerBucket(query) } returns emptyMap()

        val metrics = service.getClickMetrics(query)

        assertEquals(listOf(0L, 0L, 0L), metrics.buckets.map { it.count })
        assertEquals(0, metrics.total)
    }

    @Test
    fun `store failure propagates`() {
        every { port.countsPerBucket(query) } throws ClickStoreUnavailableException("down")

        assertThrows<ClickStoreUnavailableException> { service.getClickMetrics(query) }
    }
}
