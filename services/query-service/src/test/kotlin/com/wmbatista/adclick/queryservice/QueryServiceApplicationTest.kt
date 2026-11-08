package com.wmbatista.adclick.queryservice

import com.wmbatista.adclick.queryservice.ClickHouseTestContainer.insert
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment.RANDOM_PORT
import org.springframework.core.env.Environment
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import java.time.Instant

@SpringBootTest(webEnvironment = RANDOM_PORT)
class QueryServiceApplicationTest(@Autowired private val env: Environment) {

    companion object {
        @JvmStatic
        @DynamicPropertySource
        fun clickHouse(registry: DynamicPropertyRegistry) {
            val container = ClickHouseTestContainer.container
            registry.add("spring.datasource.url") { container.jdbcUrl }
            registry.add("spring.datasource.username") { container.username }
            registry.add("spring.datasource.password") { container.password }
        }
    }

    @BeforeEach
    fun emptyTable() = ClickHouseTestContainer.truncate()

    @Test
    fun `serves zero-filled buckets from ClickHouse`() {
        insert("ad-123", Instant.parse("2026-11-08T10:00:00Z"), 2)
        insert("ad-123", Instant.parse("2026-11-08T10:00:00Z"), 3)
        insert("ad-123", Instant.parse("2026-11-08T10:02:00Z"), 2)

        val response = env.httpGet("/ads/ad-123/clicks?from=2026-11-08T10:00:00Z&to=2026-11-08T10:03:00Z")

        assertEquals(200, response.statusCode())
        val body = response.body().replace(" ", "")
        assertTrue(body.contains(""""total":7"""), body)
        assertTrue(
            body.contains(
                """"buckets":[{"start":"2026-11-08T10:00:00Z","count":5},""" +
                    """{"start":"2026-11-08T10:01:00Z","count":0},""" +
                    """{"start":"2026-11-08T10:02:00Z","count":2}]""",
            ),
            body,
        )
    }

    @Test
    fun `health is UP`() {
        val response = env.httpGet("/actuator/health")

        assertEquals(200, response.statusCode())
        assertTrue(response.body().contains(""""status":"UP""""), response.body())
    }
}
