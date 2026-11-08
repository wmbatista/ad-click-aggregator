package com.wmbatista.adclick.clickservice.config

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.config.YamlPropertiesFactoryBean
import org.springframework.boot.convert.DurationStyle
import org.springframework.core.io.ClassPathResource
import java.util.Properties

/**
 * Guards the producer settings in application.yml. In particular, Kafka must give up on a send
 * before the HTTP request does, so a click answered with 503 is never written to the topic later
 * (the client's retry would then be counted twice).
 */
class ProducerTimeoutConfigurationTest {

    private val properties: Properties = YamlPropertiesFactoryBean()
        .apply { setResources(ClassPathResource("application.yml")) }
        .getObject()!!

    private fun producerProperty(name: String): String =
        requireNotNull(properties.getProperty("spring.kafka.producer.properties.$name")) { "missing producer property $name" }

    private fun producerMillis(name: String): Long = producerProperty(name).toLong()

    @Test
    fun `delivery timeout does not exceed send timeout`() {
        val sendTimeout = DurationStyle.detectAndParse(properties.getProperty("click-service.send-timeout"))

        assertTrue(producerMillis("delivery.timeout.ms") <= sendTimeout.toMillis())
    }

    @Test
    fun `delivery timeout covers linger plus request timeout`() {
        assertTrue(producerMillis("delivery.timeout.ms") >= producerMillis("linger.ms") + producerMillis("request.timeout.ms"))
    }

    @Test
    fun `producer is durable and idempotent`() {
        assertEquals("all", properties.getProperty("spring.kafka.producer.acks"))
        assertEquals("true", producerProperty("enable.idempotence"))
    }
}
