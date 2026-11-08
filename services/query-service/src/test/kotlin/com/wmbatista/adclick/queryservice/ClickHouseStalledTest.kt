package com.wmbatista.adclick.queryservice

import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.context.SpringBootTest.WebEnvironment.RANDOM_PORT
import org.springframework.core.env.Environment
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import java.time.Duration
import java.time.Instant

/** ClickHouse accepts connections but never answers (paused container): requests must still end in 503 quickly. */
@SpringBootTest(webEnvironment = RANDOM_PORT)
class ClickHouseStalledTest(@Autowired private val env: Environment) {

    companion object {
        private val container = ClickHouseTestContainer.newContainer().apply { start() }

        @JvmStatic
        @DynamicPropertySource
        fun clickHouse(registry: DynamicPropertyRegistry) {
            registry.add("spring.datasource.url") { container.jdbcUrl }
            registry.add("spring.datasource.username") { container.username }
            registry.add("spring.datasource.password") { container.password }
        }

        @JvmStatic
        @AfterAll
        fun stopContainer() {
            runCatching { container.dockerClient.unpauseContainerCmd(container.containerId).exec() }
            container.stop()
        }
    }

    private val path = "/ads/ad-123/clicks?from=2026-11-08T10:00:00Z&to=2026-11-08T10:03:00Z"

    @Test
    fun `answers 503 within 10 seconds when ClickHouse stops responding`() {
        assertEquals(200, env.httpGet(path).statusCode(), "warm-up request against a healthy ClickHouse")
        container.dockerClient.pauseContainerCmd(container.containerId).exec()
        val start = Instant.now()

        val response = env.httpGet(path)

        assertEquals(503, response.statusCode())
        val elapsed = Duration.between(start, Instant.now())
        assertTrue(elapsed < Duration.ofSeconds(10), "took $elapsed")
    }
}
