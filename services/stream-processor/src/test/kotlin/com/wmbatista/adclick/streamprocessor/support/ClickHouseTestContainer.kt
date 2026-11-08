package com.wmbatista.adclick.streamprocessor.support

import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.datasource.DriverManagerDataSource
import org.testcontainers.clickhouse.ClickHouseContainer
import org.testcontainers.utility.MountableFile
import java.nio.file.Path
import java.time.Instant
import javax.sql.DataSource

/**
 * One ClickHouse container shared by every test class, with the schema loaded the same way Compose
 * loads it: the shared DDL file mounted as an init script. Ryuk removes the container after the run.
 */
object ClickHouseTestContainer {

    const val USER = "ad_clicks"
    const val PASSWORD = "ad_clicks"

    private val container: ClickHouseContainer by lazy {
        val schema = Path.of(
            requireNotNull(System.getProperty("clickhouse.schema")) { "system property clickhouse.schema is not set" },
        )
        ClickHouseContainer("clickhouse/clickhouse-server:26.8.36.3")
            .withUsername(USER)
            .withPassword(PASSWORD)
            .withDatabaseName("ad_clicks")
            .withCopyFileToContainer(MountableFile.forHostPath(schema), "/docker-entrypoint-initdb.d/001-schema.sql")
            .also { it.start() }
    }

    fun jdbcUrl(): String = container.jdbcUrl

    fun dataSource(): DataSource = DriverManagerDataSource(jdbcUrl(), USER, PASSWORD)

    fun truncate(jdbc: JdbcTemplate) {
        jdbc.execute("TRUNCATE TABLE clicks_per_minute")
    }

    /** Current counts per (ad, minute), summed as every reader of the table must do. */
    fun clickCounts(jdbc: JdbcTemplate): Map<Pair<String, Instant>, Long> =
        jdbc.query(
            "SELECT ad_id, toUnixTimestamp(minute) AS m, sum(click_count) AS c FROM clicks_per_minute GROUP BY ad_id, m",
        ) { rs, _ -> (rs.getString("ad_id") to Instant.ofEpochSecond(rs.getLong("m"))) to rs.getLong("c") }
            .toMap()
}
