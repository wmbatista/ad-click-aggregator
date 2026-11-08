package com.wmbatista.adclick.queryservice.adapter.outbound.clickhouse

import com.wmbatista.adclick.queryservice.application.port.outbound.ClickStoreUnavailableException
import com.wmbatista.adclick.queryservice.application.port.outbound.LoadClickCountsPort
import com.wmbatista.adclick.queryservice.domain.ClickQuery
import com.wmbatista.adclick.queryservice.domain.Granularity
import org.springframework.dao.DataAccessException
import org.springframework.jdbc.core.ConnectionCallback
import org.springframework.jdbc.core.JdbcTemplate
import java.time.Duration
import java.time.Instant
import java.util.concurrent.Executor

/**
 * Reads `ad_clicks.clicks_per_minute`. The table is a SummingMergeTree whose merges are asynchronous,
 * so one `(ad_id, minute)` may still be several delta rows: counts are always `sum(click_count)`.
 *
 * Instants cross the driver as Unix epoch seconds, so neither the JVM nor the session time zone can
 * shift a bucket.
 *
 * Every read is bounded by [readTimeout], set on the connection right before the query. Configuring
 * the driver's `socket_timeout` is not enough: HikariCP resets each connection's network timeout to the
 * value the driver reported when the connection was created, and clickhouse-jdbc 0.11.0 reports 0 (no
 * limit) whatever `socket_timeout` says.
 */
class ClickHouseClickCountsAdapter(
    private val jdbcTemplate: JdbcTemplate,
    private val readTimeout: Duration,
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
            jdbcTemplate.execute(ConnectionCallback { connection ->
                connection.setNetworkTimeout(ABORT_INLINE, readTimeout.toMillis().toInt())
                connection.prepareStatement(sql).use { statement ->
                    statement.setString(1, query.adId)
                    statement.setLong(2, query.from.epochSecond)
                    statement.setLong(3, query.to.epochSecond)
                    statement.executeQuery().use { rs ->
                        buildMap {
                            while (rs.next()) put(Instant.ofEpochSecond(rs.getLong("bucket")), rs.getLong("clicks"))
                        }
                    }
                }
            })
        } catch (e: DataAccessException) {
            throw ClickStoreUnavailableException("Could not read click counts from ClickHouse", e)
        }
    }

    private fun bucketFunction(granularity: Granularity): String = when (granularity) {
        Granularity.MINUTE -> "toStartOfMinute"
        Granularity.HOUR -> "toStartOfHour"
        Granularity.DAY -> "toStartOfDay"
    }

    private companion object {
        /** Runs the driver's connection abort on the thread that hit the timeout. */
        val ABORT_INLINE = Executor { it.run() }
    }
}
