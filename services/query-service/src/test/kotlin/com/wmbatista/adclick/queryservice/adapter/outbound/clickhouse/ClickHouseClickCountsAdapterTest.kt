package com.wmbatista.adclick.queryservice.adapter.outbound.clickhouse

import com.wmbatista.adclick.queryservice.ClickHouseTestContainer
import com.wmbatista.adclick.queryservice.ClickHouseTestContainer.insert
import com.wmbatista.adclick.queryservice.application.port.outbound.ClickStoreUnavailableException
import com.wmbatista.adclick.queryservice.domain.ClickQuery
import com.wmbatista.adclick.queryservice.domain.Granularity
import com.wmbatista.adclick.queryservice.domain.Granularity.DAY
import com.wmbatista.adclick.queryservice.domain.Granularity.HOUR
import com.wmbatista.adclick.queryservice.domain.Granularity.MINUTE
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.datasource.DriverManagerDataSource
import java.time.Duration
import java.time.Instant

class ClickHouseClickCountsAdapterTest {

    private fun t(s: String) = Instant.parse(s)

    private fun query(adId: String, from: String, to: String, granularity: Granularity) =
        ClickQuery.of(adId, t(from), t(to), granularity)

    private val adapter = ClickHouseClickCountsAdapter(ClickHouseTestContainer.jdbcTemplate(), Duration.ofSeconds(3))

    @BeforeEach
    fun emptyTable() = ClickHouseTestContainer.truncate()

    @Test
    fun `sums delta rows that have not been merged`() {
        insert("ad-1", t("2026-11-08T10:00:00Z"), 2)
        insert("ad-1", t("2026-11-08T10:00:00Z"), 3)

        val counts = adapter.countsPerBucket(query("ad-1", "2026-11-08T10:00:00Z", "2026-11-08T10:01:00Z", MINUTE))

        assertEquals(mapOf(t("2026-11-08T10:00:00Z") to 5L), counts)
    }

    @Test
    fun `includes from and excludes to`() {
        insert("ad-1", t("2026-11-08T10:00:00Z"), 1)
        insert("ad-1", t("2026-11-08T10:02:00Z"), 4)
        insert("ad-1", t("2026-11-08T10:03:00Z"), 8)

        val counts = adapter.countsPerBucket(query("ad-1", "2026-11-08T10:00:00Z", "2026-11-08T10:03:00Z", MINUTE))

        assertEquals(mapOf(t("2026-11-08T10:00:00Z") to 1L, t("2026-11-08T10:02:00Z") to 4L), counts)
    }

    @Test
    fun `ignores other ads`() {
        insert("ad-1", t("2026-11-08T10:00:00Z"), 1)
        insert("ad-2", t("2026-11-08T10:00:00Z"), 9)

        val counts = adapter.countsPerBucket(query("ad-1", "2026-11-08T10:00:00Z", "2026-11-08T10:01:00Z", MINUTE))

        assertEquals(mapOf(t("2026-11-08T10:00:00Z") to 1L), counts)
    }

    @Test
    fun `groups minutes into hours and days`() {
        insert("ad-1", t("2026-11-08T10:05:00Z"), 1)
        insert("ad-1", t("2026-11-08T10:59:00Z"), 2)
        insert("ad-1", t("2026-11-08T11:00:00Z"), 4)

        val hours = adapter.countsPerBucket(query("ad-1", "2026-11-08T10:00:00Z", "2026-11-08T12:00:00Z", HOUR))
        val days = adapter.countsPerBucket(query("ad-1", "2026-11-08T00:00:00Z", "2026-11-09T00:00:00Z", DAY))

        assertEquals(mapOf(t("2026-11-08T10:00:00Z") to 3L, t("2026-11-08T11:00:00Z") to 4L), hours)
        assertEquals(mapOf(t("2026-11-08T00:00:00Z") to 7L), days)
    }

    @Test
    fun `unreachable ClickHouse throws ClickStoreUnavailableException`() {
        val unreachable = ClickHouseClickCountsAdapter(
            JdbcTemplate(DriverManagerDataSource("jdbc:clickhouse://localhost:1/ad_clicks", "ad_clicks", "ad_clicks")),
            Duration.ofSeconds(3),
        )

        assertThrows<ClickStoreUnavailableException> {
            unreachable.countsPerBucket(query("ad-1", "2026-11-08T10:00:00Z", "2026-11-08T10:01:00Z", MINUTE))
        }
    }
}
