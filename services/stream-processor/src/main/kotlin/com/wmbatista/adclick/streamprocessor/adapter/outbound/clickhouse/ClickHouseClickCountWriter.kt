package com.wmbatista.adclick.streamprocessor.adapter.outbound.clickhouse

import com.wmbatista.adclick.streamprocessor.application.port.outbound.SaveClickCountsPort
import com.wmbatista.adclick.streamprocessor.domain.ClickCount
import org.springframework.jdbc.core.JdbcTemplate

/**
 * Inserts click counts as delta rows into `clicks_per_minute`. The table is a `SummingMergeTree`,
 * so rows for the same ad and minute are added together by ClickHouse; this class never updates.
 */
class ClickHouseClickCountWriter(
    private val jdbcTemplate: JdbcTemplate,
) : SaveClickCountsPort {

    /** One JDBC batch, which the ClickHouse driver sends as a single INSERT. */
    override fun save(counts: List<ClickCount>) {
        if (counts.isEmpty()) return
        jdbcTemplate.batchUpdate(INSERT, counts, counts.size) { statement, count ->
            statement.setString(1, count.adId)
            // Bound as Unix seconds, which ClickHouse stores as-is in a DateTime column. Zone-less
            // types (LocalDateTime, Timestamp) are converted by the driver using the JVM's default
            // time zone and would shift the bucket on any host not running in UTC.
            statement.setLong(2, count.minute.epochSecond)
            statement.setLong(3, count.count)
        }
    }

    private companion object {
        const val INSERT = "INSERT INTO clicks_per_minute (ad_id, minute, click_count) VALUES (?, ?, ?)"
    }
}
