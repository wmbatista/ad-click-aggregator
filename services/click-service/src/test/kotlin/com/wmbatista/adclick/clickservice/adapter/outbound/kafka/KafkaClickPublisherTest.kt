package com.wmbatista.adclick.clickservice.adapter.outbound.kafka

import com.wmbatista.adclick.clickservice.application.port.outbound.ClickPublishingException
import com.wmbatista.adclick.clickservice.domain.Click
import com.wmbatista.adclick.event.ClickEvent
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.apache.kafka.common.KafkaException
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.support.SendResult
import java.time.Duration
import java.time.Instant
import java.util.UUID
import java.util.concurrent.CompletableFuture

class KafkaClickPublisherTest {

    private val topic = "click-events"
    private val clickId = UUID.fromString("00000000-0000-0000-0000-000000000001")
    private val timestamp = Instant.parse("2026-11-06T12:34:56Z")
    private val click = Click(clickId, "ad-42", timestamp)

    private val kafkaTemplate = mockk<KafkaTemplate<String, ClickEvent>>()

    private fun publisher(sendTimeout: Duration = Duration.ofSeconds(5)) =
        KafkaClickPublisher(kafkaTemplate, topic, sendTimeout)

    private fun stubSend(future: CompletableFuture<SendResult<String, ClickEvent>>) {
        every { kafkaTemplate.send(any<String>(), any<String>(), any<ClickEvent>()) } returns future
    }

    @Test
    fun `publish sends event to topic keyed by adId`() {
        stubSend(CompletableFuture.completedFuture(mockk()))

        publisher().publish(click)

        verify(exactly = 1) {
            kafkaTemplate.send("click-events", "ad-42", ClickEvent(clickId, "ad-42", timestamp))
        }
    }

    @Test
    fun `publish fails when broker rejects the record`() {
        stubSend(CompletableFuture.failedFuture(KafkaException("boom")))

        assertThrows<ClickPublishingException> { publisher().publish(click) }
    }

    @Test
    fun `publish fails when send throws synchronously`() {
        every { kafkaTemplate.send(any<String>(), any<String>(), any<ClickEvent>()) } throws
            KafkaException("metadata not available after max.block.ms")

        assertThrows<ClickPublishingException> { publisher().publish(click) }
    }

    @Test
    fun `publish fails when KafkaTemplate wraps a synchronous send failure`() {
        // What KafkaTemplate actually throws when the broker is unreachable and max.block.ms elapses.
        every { kafkaTemplate.send(any<String>(), any<String>(), any<ClickEvent>()) } throws
            org.springframework.kafka.KafkaException("Send failed", KafkaException("metadata timeout"))

        assertThrows<ClickPublishingException> { publisher().publish(click) }
    }

    @Test
    fun `publish fails when ack does not arrive within timeout`() {
        stubSend(CompletableFuture())

        assertThrows<ClickPublishingException> { publisher(Duration.ofMillis(50)).publish(click) }
    }

    @Test
    fun `publish restores interrupt flag when interrupted`() {
        stubSend(CompletableFuture())
        Thread.currentThread().interrupt()

        assertThrows<ClickPublishingException> { publisher().publish(click) }
        assertTrue(Thread.interrupted(), "interrupt flag should be restored")
    }
}
