package com.wmbatista.adclick.clickservice.config

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import java.time.Duration

class ClickServiceConfigurationTest {

    @Test
    fun `click events topic has configured partitions and replication`() {
        val properties = ClickServiceProperties(
            topic = ClickServiceProperties.Topic(name = "click-events", partitions = 12, replicationFactor = 1),
            sendTimeout = Duration.ofSeconds(6),
        )

        val topic = ClickServiceConfiguration().clickEventsTopic(properties)

        assertEquals("click-events", topic.name())
        assertEquals(12, topic.numPartitions())
        assertEquals(1.toShort(), topic.replicationFactor())
    }
}
