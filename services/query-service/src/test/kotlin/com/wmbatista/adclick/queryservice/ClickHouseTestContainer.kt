package com.wmbatista.adclick.queryservice

import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.datasource.DriverManagerDataSource
import org.testcontainers.clickhouse.ClickHouseContainer
import org.testcontainers.utility.MountableFile
import java.time.Instant

/**
 * One ClickHouse per test JVM, with the same image and schema as Compose
 * (`infra/clickhouse/init/001-schema.sql`, passed in by Gradle as the `clickhouse.schema` property).
 */
object ClickHouseTestContainer {

    const val IMAGE = "clickhouse/clickhouse-server:26.8.36.3"

    val container: ClickHouseContainer by lazy {
        val schema = requireNotNull(System.getProperty("clickhouse.schema")) {
            "System property clickhouse.schema is not set; run the tests through Gradle"
        }
        ClickHouseContainer(IMAGE)
            .withDatabaseName("ad_clicks")
            .withUsername("ad_clicks")
            .withPassword("ad_clicks")
            .withCopyFileToContainer(MountableFile.forHostPath(schema), "/docker-entrypoint-initdb.d/001-schema.sql")
            .apply { start() }
    }

    private val jdbcTemplate: JdbcTemplate by lazy { jdbcTemplate() }

    fun jdbcTemplate(): JdbcTemplate =
        JdbcTemplate(DriverManagerDataSource(container.jdbcUrl, container.username, container.password))

    /** Inserts one delta row in its own INSERT, so each call creates a separate, unmerged part. */
    fun insert(adId: String, minute: Instant, count: Long) {
        jdbcTemplate.update(
            "INSERT INTO ad_clicks.clicks_per_minute (ad_id, minute, click_count) " +
                "VALUES ('$adId', toDateTime(${minute.epochSecond}, 'UTC'), $count)",
        )
    }

    fun truncate() {
        jdbcTemplate.update("TRUNCATE TABLE ad_clicks.clicks_per_minute")
    }
}
