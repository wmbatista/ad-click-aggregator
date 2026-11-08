package com.wmbatista.adclick.clickservice.config

import com.wmbatista.adclick.event.ClickEvent
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertDoesNotThrow
import org.junit.jupiter.api.assertThrows
import org.springframework.boot.kafka.autoconfigure.KafkaProperties
import org.springframework.kafka.core.KafkaTemplate
import java.time.Duration

class ClickServiceConfigurationTest {

    private val properties = ClickServiceProperties(
        topic = ClickServiceProperties.Topic(name = "click-events", partitions = 12, replicationFactor = 1),
        sendTimeout = Duration.ofSeconds(6),
    )
    private val kafkaTemplate = mockk<KafkaTemplate<String, ClickEvent>>()

    private fun kafkaProperties(deliveryTimeoutMs: String?) = KafkaProperties().apply {
        deliveryTimeoutMs?.let { producer.properties["delivery.timeout.ms"] = it }
    }

    @Test
    fun `click events topic has configured partitions and replication`() {
        val topic = ClickServiceConfiguration().clickEventsTopic(properties)

        assertEquals("click-events", topic.name())
        assertEquals(12, topic.numPartitions())
        assertEquals(1.toShort(), topic.replicationFactor())
    }

    @Test
    fun `publish click port accepts delivery timeout within send timeout`() {
        assertDoesNotThrow {
            ClickServiceConfiguration().publishClickPort(kafkaTemplate, properties, kafkaProperties("5000"))
        }
    }

    @Test
    fun `publish click port rejects delivery timeout longer than send timeout`() {
        assertThrows<IllegalStateException> {
            ClickServiceConfiguration().publishClickPort(kafkaTemplate, properties, kafkaProperties("30000"))
        }
    }

    @Test
    fun `publish click port rejects unset delivery timeout since kafka default is 120s`() {
        assertThrows<IllegalStateException> {
            ClickServiceConfiguration().publishClickPort(kafkaTemplate, properties, kafkaProperties(null))
        }
    }
}
