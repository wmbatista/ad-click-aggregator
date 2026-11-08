package com.wmbatista.adclick.streamprocessor.domain

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.time.Instant

class ClickAggregationTest {

    @Test
    fun `clicks in the same minute and ad are counted together`() {
        val clicks = listOf(
            Click("ad-1", at("10:00:00")),
            Click("ad-1", at("10:00:30")),
            Click("ad-1", at("10:00:59.999")),
        )

        assertEquals(listOf(ClickCount("ad-1", at("10:00:00"), 3)), aggregatePerMinute(clicks))
    }

    @Test
    fun `minute boundary starts a new bucket`() {
        val clicks = listOf(Click("ad-1", at("10:00:59.999")), Click("ad-1", at("10:01:00")))

        assertEquals(
            listOf(ClickCount("ad-1", at("10:00:00"), 1), ClickCount("ad-1", at("10:01:00"), 1)),
            aggregatePerMinute(clicks),
        )
    }

    @Test
    fun `different ads are counted separately`() {
        val clicks = listOf(
            Click("ad-2", at("10:00:10")),
            Click("ad-1", at("10:00:20")),
            Click("ad-2", at("10:00:40")),
        )

        assertEquals(
            listOf(ClickCount("ad-1", at("10:00:00"), 1), ClickCount("ad-2", at("10:00:00"), 2)),
            aggregatePerMinute(clicks),
        )
    }

    @Test
    fun `no clicks gives no counts`() {
        assertEquals(emptyList<ClickCount>(), aggregatePerMinute(emptyList()))
    }

    private fun at(time: String): Instant = Instant.parse("2026-11-08T${time}Z")
}
