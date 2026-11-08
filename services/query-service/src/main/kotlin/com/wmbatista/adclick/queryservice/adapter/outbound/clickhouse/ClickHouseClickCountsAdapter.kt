package com.wmbatista.adclick.queryservice.adapter.outbound.clickhouse

import com.wmbatista.adclick.queryservice.application.port.outbound.ClickStoreUnavailableException
import com.wmbatista.adclick.queryservice.application.port.outbound.LoadClickCountsPort
import com.wmbatista.adclick.queryservice.domain.ClickQuery
import com.wmbatista.adclick.queryservice.domain.Granularity
import org.springframework.dao.DataAccessException
import org.springframework.jdbc.core.JdbcTemplate
import java.time.Instant

/**
 * Reads `ad_clicks.clicks_per_minute`. The table is a SummingMergeTree whose merges are asynchronous,
 * so one `(ad_id, minute)` may still be several delta rows: counts are always `sum(click_count)`.
 *
 * Instants cross the driver as Unix epoch seconds, so neither the JVM nor the session time zone can
 * shift a bucket.
 */
class ClickHouseClickCountsAdapter(
    private val jdbcTemplate: JdbcTemplate,
) : LoadClickCountsPort {

    override fun countsPerBucket(query: ClickQuery): Map<Instant, Long> {
        val sql = """
            SELECT toUnixTimestamp(${bucketFunction(query.granularity)}(minute)) AS bucket,
                   sum(click_count) AS clicks
            FROM ad_clicks.clicks_per_minute
            WHERE ad_id = ?
              AND minute >= toDateTime(?, 'UTC')
              AND minute < toDateTime(?, 'UTC')
            GROUP BY bucket
        """.trimIndent()
        return try {
            jdbcTemplate.query(sql, { rs, _ ->
                Instant.ofEpochSecond(rs.getLong("bucket")) to rs.getLong("clicks")
            }, query.adId, query.from.epochSecond, query.to.epochSecond).toMap()
        } catch (e: DataAccessException) {
            throw ClickStoreUnavailableException("Could not read click counts from ClickHouse", e)
        }
    }

    private fun bucketFunction(granularity: Granularity): String = when (granularity) {
        Granularity.MINUTE -> "toStartOfMinute"
        Granularity.HOUR -> "toStartOfHour"
        Granularity.DAY -> "toStartOfDay"
    }
}
