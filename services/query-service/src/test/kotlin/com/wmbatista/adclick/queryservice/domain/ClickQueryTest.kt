package com.wmbatista.adclick.queryservice.domain

import com.wmbatista.adclick.queryservice.domain.Granularity.DAY
import com.wmbatista.adclick.queryservice.domain.Granularity.HOUR
import com.wmbatista.adclick.queryservice.domain.Granularity.MINUTE
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import java.time.Instant
import org.junit.jupiter.api.Assertions.assertEquals

class ClickQueryTest {

    private fun t(s: String) = Instant.parse(s)

    @Test
    fun `truncates from down and rounds to up to the granularity`() {
        val q = ClickQuery.of("ad-1", t("2026-11-08T10:00:30.500Z"), t("2026-11-08T10:02:00.001Z"), MINUTE)

        assertEquals(t("2026-11-08T10:00:00Z"), q.from)
        assertEquals(t("2026-11-08T10:03:00Z"), q.to)
        assertEquals(
            listOf(t("2026-11-08T10:00:00Z"), t("2026-11-08T10:01:00Z"), t("2026-11-08T10:02:00Z")),
            q.bucketStarts,
        )
    }

    @Test
    fun `an aligned to is kept`() {
        val q = ClickQuery.of("ad-1", t("2026-11-08T10:00:00Z"), t("2026-11-08T10:02:00Z"), MINUTE)

        assertEquals(t("2026-11-08T10:02:00Z"), q.to)
        assertEquals(2, q.bucketStarts.size)
    }

    @Test
    fun `hour buckets are UTC-aligned`() {
        val q = ClickQuery.of("ad-1", t("2026-11-08T10:15:00Z"), t("2026-11-08T12:00:01Z"), HOUR)

        assertEquals(
            listOf(t("2026-11-08T10:00:00Z"), t("2026-11-08T11:00:00Z"), t("2026-11-08T12:00:00Z")),
            q.bucketStarts,
        )
    }

    @Test
    fun `day buckets are UTC-aligned`() {
        val q = ClickQuery.of("ad-1", t("2026-11-08T23:00:00Z"), t("2026-11-09T01:00:00Z"), DAY)

        assertEquals(listOf(t("2026-11-08T00:00:00Z"), t("2026-11-09T00:00:00Z")), q.bucketStarts)
    }

    @Test
    fun `allows exactly 1440 buckets`() {
        val q = ClickQuery.of("ad-1", t("2026-11-08T00:00:00Z"), t("2026-11-09T00:00:00Z"), MINUTE)

        assertEquals(ClickQuery.MAX_BUCKETS, q.bucketStarts.size)
    }

    @Test
    fun `rejects 1441 buckets`() {
        assertThrows<InvalidClickQueryException> {
            ClickQuery.of("ad-1", t("2026-11-08T00:00:00Z"), t("2026-11-09T00:01:00Z"), MINUTE)
        }
    }

    @ParameterizedTest
    @CsvSource(
        "2026-11-08T10:00:00Z, 2026-11-08T10:00:00Z",
        "2026-11-08T10:03:00Z, 2026-11-08T10:00:00Z",
        // Would be a valid one-minute bucket after normalization, but the raw input is already empty.
        "2026-11-08T10:00:30Z, 2026-11-08T10:00:30Z",
    )
    fun `rejects from not before to`(from: String, to: String) {
        assertThrows<InvalidClickQueryException> { ClickQuery.of("ad-1", t(from), t(to), MINUTE) }
    }

    @Test
    fun `rejects a blank adId`() {
        assertThrows<InvalidClickQueryException> {
            ClickQuery.of(" ", t("2026-11-08T10:00:00Z"), t("2026-11-08T10:01:00Z"), MINUTE)
        }
    }

    @ParameterizedTest
    @CsvSource(
        "1969-12-31T23:59:00Z, 1970-01-01T00:01:00Z",
        "2105-12-31T23:59:00Z, 2106-01-01T00:01:00Z",
    )
    fun `rejects ranges outside the ClickHouse DateTime range`(from: String, to: String) {
        assertThrows<InvalidClickQueryException> { ClickQuery.of("ad-1", t(from), t(to), MINUTE) }
    }
}
