package com.wmbatista.adclick.streamprocessor

import com.wmbatista.adclick.streamprocessor.support.ClickHouseTestContainer
import com.wmbatista.adclick.streamprocessor.support.ClickHouseTestContainer.clickCounts
import org.apache.kafka.clients.admin.Admin
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.admin.OffsetSpec
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringSerializer
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.fail
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.testcontainers.service.connection.ServiceConnection
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.test.context.DynamicPropertyRegistry
import org.springframework.test.context.DynamicPropertySource
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.kafka.KafkaContainer
import java.time.Duration
import java.time.Instant
import java.util.UUID

/**
 * The whole pipeline against real Kafka and ClickHouse: records in, summed counts out, and offsets
 * committed only once the counts are stored.
 */
@Testcontainers
@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.NONE,
    properties = [
        "stream-processor.retry.initial-interval=100ms",
        "stream-processor.retry.max-interval=500ms",
    ],
)
class StreamProcessorIntegrationTest {

    private val jdbc = JdbcTemplate(ClickHouseTestContainer.dataSource())

    @BeforeEach
    fun startFromEmptyTable() {
        awaitAllCommitted()
        ClickHouseTestContainer.truncate(jdbc)
    }

    @Test
    fun `clicks are counted per ad and minute and offsets committed`() {
        sendClick("ad-1", "10:00:05")
        sendClick("ad-1", "10:00:50")
        sendClick("ad-1", "10:01:00")
        sendClick("ad-2", "10:00:30")
        send("ad-1", "not json")

        awaitUntil("counts stored") {
            clickCounts(jdbc) == mapOf(
                ("ad-1" to at("10:00:00")) to 2L,
                ("ad-1" to at("10:01:00")) to 1L,
                ("ad-2" to at("10:00:00")) to 1L,
            )
        }
        awaitAllCommitted() // the undeserializable record was skipped, not blocking its partition
    }

    @Test
    fun `clickhouse outage holds offsets until the insert succeeds`() {
        jdbc.execute("RENAME TABLE clicks_per_minute TO clicks_per_minute_off")
        try {
            val committedBefore = committedOffsets()
            sendClick("ad-3", "10:00:10")
            sendClick("ad-3", "10:00:20")

            Thread.sleep(3_000) // several retries at 100-500 ms
            assertEquals(committedBefore, committedOffsets(), "offsets committed while the insert was failing")
        } finally {
            jdbc.execute("RENAME TABLE clicks_per_minute_off TO clicks_per_minute")
        }

        awaitUntil("counts stored after recovery") { clickCounts(jdbc) == mapOf(("ad-3" to at("10:00:00")) to 2L) }
        awaitAllCommitted()
    }

    private fun sendClick(adId: String, time: String) =
        send(adId, """{"clickId":"${UUID.randomUUID()}","adId":"$adId","timestamp":"${at(time)}"}""")

    private fun send(key: String, value: String) {
        producer.send(ProducerRecord(TOPIC, key, value)).get()
    }

    private fun committedOffsets(): Map<TopicPartition, Long> =
        admin.listConsumerGroupOffsets("stream-processor").partitionsToOffsetAndMetadata().get()
            .filterValues { it != null }
            .mapValues { it.value.offset() }

    private fun endOffsets(): Map<TopicPartition, Long> {
        val partitions = admin.describeTopics(listOf(TOPIC)).allTopicNames().get().getValue(TOPIC).partitions()
            .associate { TopicPartition(TOPIC, it.partition()) to OffsetSpec.latest() }
        return admin.listOffsets(partitions).all().get().mapValues { it.value.offset() }
    }

    /** Every partition that has records has its offset committed up to the end. */
    private fun awaitAllCommitted() =
        awaitUntil("all offsets committed") {
            endOffsets().filterValues { it > 0 } == committedOffsets().filterValues { it > 0 }
        }

    private fun awaitUntil(what: String, condition: () -> Boolean) {
        val deadline = Instant.now().plus(Duration.ofSeconds(30))
        while (Instant.now().isBefore(deadline)) {
            if (condition()) return
            Thread.sleep(200)
        }
        fail<Unit>("timed out waiting for: $what")
    }

    private fun at(time: String): Instant = Instant.parse("2026-11-08T${time}Z")

    companion object {
        private const val TOPIC = "click-events"

        @Container
        @ServiceConnection
        @JvmField
        val kafka = KafkaContainer("apache/kafka:4.2.2")

        private lateinit var admin: Admin
        private lateinit var producer: KafkaProducer<String, String>

        @JvmStatic
        @DynamicPropertySource
        fun clickHouse(registry: DynamicPropertyRegistry) {
            registry.add("spring.datasource.url", ClickHouseTestContainer::jdbcUrl)
            registry.add("spring.datasource.username") { ClickHouseTestContainer.USER }
            registry.add("spring.datasource.password") { ClickHouseTestContainer.PASSWORD }
        }

        /** The service never creates the topic (the infrastructure does), so the test creates it. */
        @JvmStatic
        @BeforeAll
        fun createTopicAndClients() {
            admin = Admin.create(mapOf(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG to kafka.bootstrapServers))
            admin.createTopics(listOf(NewTopic(TOPIC, 3, 1.toShort()))).all().get()
            producer = KafkaProducer(
                mapOf<String, Any>(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG to kafka.bootstrapServers),
                StringSerializer(),
                StringSerializer(),
            )
        }

        @JvmStatic
        @AfterAll
        fun closeClients() {
            producer.close()
            admin.close()
        }
    }
}
