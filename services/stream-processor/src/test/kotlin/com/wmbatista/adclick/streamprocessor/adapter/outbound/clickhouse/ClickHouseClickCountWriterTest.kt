package com.wmbatista.adclick.streamprocessor.adapter.outbound.clickhouse

import com.wmbatista.adclick.streamprocessor.domain.ClickCount
import com.wmbatista.adclick.streamprocessor.support.ClickHouseTestContainer
import com.wmbatista.adclick.streamprocessor.support.ClickHouseTestContainer.clickCounts
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.springframework.dao.DataAccessException
import org.springframework.jdbc.core.JdbcTemplate
import java.time.Instant
import java.time.ZoneId
import java.time.ZoneOffset

class ClickHouseClickCountWriterTest {

    private val jdbc = JdbcTemplate(ClickHouseTestContainer.dataSource())
    private val writer = ClickHouseClickCountWriter(jdbc)

    @BeforeEach
    fun emptyTable() {
        ClickHouseTestContainer.truncate(jdbc)
    }

    @Test
    fun `inserted counts can be read back by ad and utc minute`() {
        writer.save(listOf(ClickCount("ad-1", at("10:00"), 3), ClickCount("ad-2", at("10:01"), 1)))

        assertEquals(mapOf(("ad-1" to at("10:00")) to 3L, ("ad-2" to at("10:01")) to 1L), clickCounts(jdbc))
    }

    @Test
    fun `deltas for the same ad and minute add up`() {
        writer.save(listOf(ClickCount("ad-1", at("10:00"), 3)))
        writer.save(listOf(ClickCount("ad-1", at("10:00"), 4)))

        assertEquals(mapOf(("ad-1" to at("10:00")) to 7L), clickCounts(jdbc))
    }

    @Test
    fun `a batch is written by a single insert query`() {
        val before = finishedInserts()

        writer.save((1..50).map { ClickCount("ad-$it", at("10:00"), 1) })

        val after = finishedInserts()
        assertEquals(1L, after.first - before.first, "insert queries")
        assertEquals(50L, after.second - before.second, "rows written")
    }

    /** (insert queries, rows written) into the table so far, from ClickHouse's query log. */
    private fun finishedInserts(): Pair<Long, Long> {
        jdbc.execute("SYSTEM FLUSH LOGS")
        return jdbc.queryForObject(
            "SELECT count(), sum(written_rows) FROM system.query_log WHERE type = 'QueryFinish' " +
                "AND query_kind = 'Insert' AND has(tables, 'ad_clicks.clicks_per_minute')",
        ) { rs, _ -> rs.getLong(1) to rs.getLong(2) }!!
    }

    @Test
    fun `jvm time zone is not utc in tests`() {
        // Guards the time zone test above: it only proves something if the JVM default is not UTC.
        assertNotEquals(ZoneOffset.UTC, ZoneId.systemDefault().rules.getOffset(Instant.now()))
    }

    @Test
    fun `empty list writes nothing`() {
        writer.save(emptyList())

        assertTrue(clickCounts(jdbc).isEmpty())
    }

    @Test
    fun `missing table fails the save`() {
        jdbc.execute("RENAME TABLE clicks_per_minute TO clicks_per_minute_off")
        try {
            assertThrows<DataAccessException> { writer.save(listOf(ClickCount("ad-1", at("10:00"), 1))) }
        } finally {
            jdbc.execute("RENAME TABLE clicks_per_minute_off TO clicks_per_minute")
        }
    }

    private fun at(time: String): Instant = Instant.parse("2026-11-08T$time:00Z")
}
