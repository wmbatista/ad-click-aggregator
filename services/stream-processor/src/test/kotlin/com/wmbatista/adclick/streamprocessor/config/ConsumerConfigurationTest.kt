package com.wmbatista.adclick.streamprocessor.config

import com.wmbatista.adclick.event.ClickEvent
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.header.internals.RecordHeaders
import org.apache.kafka.common.serialization.Deserializer
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.kafka.autoconfigure.KafkaProperties
import org.springframework.boot.test.context.SpringBootTest
import java.time.Instant
import java.util.UUID

/**
 * Pins the consumer settings from application.yml against the shared contract. Listeners are not
 * started, so no broker or ClickHouse is needed.
 */
@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.NONE,
    properties = ["spring.kafka.listener.auto-startup=false"],
)
class ConsumerConfigurationTest {

    @Autowired
    private lateinit var kafkaProperties: KafkaProperties

    private lateinit var deserializer: Deserializer<*>

    @BeforeEach
    fun buildDeserializer() {
        val props = kafkaProperties.buildConsumerProperties()
        val valueDeserializer = props[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] as Class<*>
        deserializer = valueDeserializer.getDeclaredConstructor().newInstance() as Deserializer<*>
        deserializer.configure(props, false)
    }

    @Test
    fun `consumer settings match the contract`() {
        val props = kafkaProperties.buildConsumerProperties()

        assertEquals("stream-processor", props[ConsumerConfig.GROUP_ID_CONFIG])
        assertEquals("earliest", props[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG])
        assertEquals(false, props[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG])
        assertEquals(500, props[ConsumerConfig.MAX_POLL_RECORDS_CONFIG])
    }

    @Test
    fun `click event json without type headers deserializes`() {
        assertEquals(EXPECTED_EVENT, deserialize(EVENT_JSON))
    }

    @Test
    fun `unknown json fields are ignored`() {
        val json = EVENT_JSON.replace("}", ""","campaign":"x"}""")

        assertEquals(EXPECTED_EVENT, deserialize(json))
    }

    @Test
    fun `malformed value deserializes to null`() {
        assertNull(deserialize("not json"))
    }

    @Test
    fun `missing adId deserializes to null`() {
        assertNull(deserialize("""{"clickId":"$CLICK_ID","timestamp":"2026-11-06T12:34:56Z"}"""))
    }

    private fun deserialize(value: String): Any? =
        deserializer.deserialize("click-events", RecordHeaders(), value.toByteArray())

    private companion object {
        const val CLICK_ID = "00000000-0000-0000-0000-000000000001"
        const val EVENT_JSON = """{"clickId":"$CLICK_ID","adId":"ad-42","timestamp":"2026-11-06T12:34:56Z"}"""
        val EXPECTED_EVENT = ClickEvent(UUID.fromString(CLICK_ID), "ad-42", Instant.parse("2026-11-06T12:34:56Z"))
    }
}
