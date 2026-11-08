package com.wmbatista.adclick.clickservice.config

import com.wmbatista.adclick.clickservice.adapter.outbound.kafka.KafkaClickPublisher
import com.wmbatista.adclick.clickservice.application.port.inbound.RegisterClickUseCase
import com.wmbatista.adclick.clickservice.application.port.outbound.ClickIdGenerator
import com.wmbatista.adclick.clickservice.application.port.outbound.PublishClickPort
import com.wmbatista.adclick.clickservice.application.service.RegisterClickService
import com.wmbatista.adclick.event.ClickEvent
import org.apache.kafka.clients.admin.NewTopic
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.config.TopicBuilder
import org.springframework.kafka.core.KafkaTemplate
import java.time.Clock
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
    ): PublishClickPort = KafkaClickPublisher(kafkaTemplate, properties.topic.name, properties.sendTimeout)

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
