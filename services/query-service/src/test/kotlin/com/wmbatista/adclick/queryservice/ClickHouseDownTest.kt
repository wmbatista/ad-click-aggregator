package com.wmbatista.adclick.queryservice

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment.RANDOM_PORT
import org.springframework.core.env.Environment
import java.time.Duration
import java.time.Instant

@SpringBootTest(
    webEnvironment = RANDOM_PORT,
    properties = ["spring.datasource.url=jdbc:clickhouse://localhost:1/ad_clicks"],
)
class ClickHouseDownTest(@Autowired private val env: Environment) {

    @Test
    fun `answers 503 within 5 seconds`() {
        val start = Instant.now()

        val response = env.httpGet("/ads/ad-123/clicks?from=2026-11-08T10:00:00Z&to=2026-11-08T10:03:00Z")

        assertEquals(503, response.statusCode())
        val elapsed = Duration.between(start, Instant.now())
        assertTrue(elapsed < Duration.ofSeconds(5), "took $elapsed")
    }
}
