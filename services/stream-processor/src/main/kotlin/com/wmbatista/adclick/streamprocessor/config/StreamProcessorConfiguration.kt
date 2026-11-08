package com.wmbatista.adclick.streamprocessor.config

import com.wmbatista.adclick.streamprocessor.adapter.inbound.kafka.ClickEventsListener
import com.wmbatista.adclick.streamprocessor.adapter.outbound.clickhouse.ClickHouseClickCountWriter
import com.wmbatista.adclick.streamprocessor.application.port.inbound.AggregateClicksUseCase
import com.wmbatista.adclick.streamprocessor.application.port.outbound.SaveClickCountsPort
import com.wmbatista.adclick.streamprocessor.application.service.AggregateClicksService
import org.apache.commons.logging.LogFactory
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.ConsumerRecords
import org.springframework.context.annotation.Bean
import org.springframework.core.log.LogAccessor
import org.springframework.kafka.listener.RetryListener
import org.springframework.context.annotation.Configuration
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.kafka.listener.CommonErrorHandler
import org.springframework.kafka.listener.DefaultErrorHandler
import org.springframework.util.backoff.BackOff
import org.springframework.util.backoff.ExponentialBackOff

@Configuration
class StreamProcessorConfiguration {

    @Bean
    fun saveClickCountsPort(jdbcTemplate: JdbcTemplate): SaveClickCountsPort = ClickHouseClickCountWriter(jdbcTemplate)

    @Bean
    fun aggregateClicksUseCase(saveClickCountsPort: SaveClickCountsPort): AggregateClicksUseCase =
        AggregateClicksService(saveClickCountsPort)

    @Bean
    fun clickEventsListener(aggregateClicksUseCase: AggregateClicksUseCase): ClickEventsListener =
        ClickEventsListener(aggregateClicksUseCase)

    /**
     * Backoff between retries of a failed batch. It has no attempt or time limit: when it gives up,
     * the error handler skips the batch and commits its offsets, losing its clicks.
     */
    @Bean
    fun clickBatchBackOff(properties: StreamProcessorProperties): BackOff =
        ExponentialBackOff(properties.retry.initialInterval.toMillis(), properties.retry.multiplier).apply {
            maxInterval = properties.retry.maxInterval.toMillis()
        }

    /**
     * Picked up by Boot's listener container factory. An exception from a batch listener retries the
     * whole batch with [clickBatchBackOff], without committing, until the listener succeeds.
     */
    @Bean
    fun kafkaErrorHandler(clickBatchBackOff: BackOff): CommonErrorHandler =
        DefaultErrorHandler(clickBatchBackOff).apply { setRetryListeners(BatchRetryLogger()) }

    /**
     * Spring Kafka logs batch retries only at DEBUG, and the recoverer (which logs at ERROR) never
     * runs because retries never stop. Without this, an outage shows only as consumer lag.
     */
    private class BatchRetryLogger : RetryListener {
        // Single-record path: not used by this batch listener, but required by the interface.
        override fun failedDelivery(record: ConsumerRecord<*, *>, ex: Exception?, deliveryAttempt: Int) {
            logger.warn { "Delivery failed for ${record.topic()}-${record.partition()}@${record.offset()}: $ex" }
        }

        override fun failedDelivery(records: ConsumerRecords<*, *>, ex: Exception, deliveryAttempt: Int) {
            logger.warn(ex) {
                "Batch insert failed (attempt $deliveryAttempt, ${records.count()} records from " +
                    "${records.partitions()}); offsets not committed, retrying"
            }
        }

        private companion object {
            val logger = LogAccessor(LogFactory.getLog(StreamProcessorConfiguration::class.java))
        }
    }
}
