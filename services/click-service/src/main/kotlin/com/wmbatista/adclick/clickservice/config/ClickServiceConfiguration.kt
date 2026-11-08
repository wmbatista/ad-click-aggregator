package com.wmbatista.adclick.clickservice.config

import com.wmbatista.adclick.clickservice.adapter.outbound.kafka.KafkaClickPublisher
import com.wmbatista.adclick.clickservice.application.port.inbound.RegisterClickUseCase
import com.wmbatista.adclick.clickservice.application.port.outbound.ClickIdGenerator
import com.wmbatista.adclick.clickservice.application.port.outbound.PublishClickPort
import com.wmbatista.adclick.clickservice.application.service.RegisterClickService
import com.wmbatista.adclick.event.ClickEvent
import org.apache.kafka.clients.admin.NewTopic
import org.springframework.boot.kafka.autoconfigure.KafkaProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.config.TopicBuilder
import org.springframework.kafka.core.KafkaTemplate
import java.time.Clock
import java.time.Duration
import java.util.UUID

@Configuration
class ClickServiceConfiguration {

    @Bean
    fun clock(): Clock = Clock.systemUTC()

    @Bean
    fun clickIdGenerator(): ClickIdGenerator = ClickIdGenerator(UUID::randomUUID)

    @Bean
    fun publishClickPort(
        kafkaTemplate: KafkaTemplate<String, ClickEvent>,
        properties: ClickServiceProperties,
        kafkaProperties: KafkaProperties,
    ): PublishClickPort {
        checkDeliveryTimeoutWithinSendTimeout(kafkaProperties, properties.sendTimeout)
        return KafkaClickPublisher(kafkaTemplate, properties.topic.name, properties.sendTimeout)
    }

    /**
     * Fails startup if Kafka could keep retrying a send after the request has already answered 503,
     * which would make "503, then written anyway" (and a double count on the client's retry) routine.
     */
    private fun checkDeliveryTimeoutWithinSendTimeout(kafkaProperties: KafkaProperties, sendTimeout: Duration) {
        val deliveryTimeoutMs = kafkaProperties.producer.properties["delivery.timeout.ms"]?.toLong()
            ?: KAFKA_DEFAULT_DELIVERY_TIMEOUT_MS
        check(deliveryTimeoutMs <= sendTimeout.toMillis()) {
            "spring.kafka.producer.properties.delivery.timeout.ms ($deliveryTimeoutMs ms) must not exceed " +
                "click-service.send-timeout ($sendTimeout)"
        }
    }

    private companion object {
        const val KAFKA_DEFAULT_DELIVERY_TIMEOUT_MS = 120_000L
    }

    @Bean
    fun registerClickUseCase(
        publishClickPort: PublishClickPort,
        clock: Clock,
        clickIdGenerator: ClickIdGenerator,
    ): RegisterClickUseCase = RegisterClickService(publishClickPort, clock, clickIdGenerator)

    @Bean
    fun clickEventsTopic(properties: ClickServiceProperties): NewTopic =
        TopicBuilder.name(properties.topic.name)
            .partitions(properties.topic.partitions)
            .replicas(properties.topic.replicationFactor.toInt())
            .build()
}
