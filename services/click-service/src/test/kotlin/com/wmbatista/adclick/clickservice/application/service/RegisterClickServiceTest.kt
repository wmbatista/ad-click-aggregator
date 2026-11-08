package com.wmbatista.adclick.clickservice.application.service

import com.wmbatista.adclick.clickservice.application.port.outbound.ClickIdGenerator
import com.wmbatista.adclick.clickservice.application.port.outbound.ClickPublishingException
import com.wmbatista.adclick.clickservice.application.port.outbound.PublishClickPort
import com.wmbatista.adclick.clickservice.domain.Click
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import java.time.Clock
import java.time.Instant
import java.time.ZoneOffset
import java.util.UUID

class RegisterClickServiceTest {

    private val now = Instant.parse("2026-11-06T12:34:56Z")
    private val clickId = UUID.fromString("00000000-0000-0000-0000-000000000001")
    private val clock = Clock.fixed(now, ZoneOffset.UTC)
    private val idGenerator = ClickIdGenerator { clickId }

    private class RecordingPublishClickPort : PublishClickPort {
        val published = mutableListOf<Click>()
        override fun publish(click: Click) {
            published += click
        }
    }

    @Test
    fun `register publishes click with generated id, given adId and current time`() {
        val port = RecordingPublishClickPort()
        val service = RegisterClickService(port, clock, idGenerator)

        val click = service.register("ad-42")

        val expected = Click(clickId, "ad-42", now)
        assertEquals(expected, click)
        assertEquals(listOf(expected), port.published)
    }

    @Test
    fun `register propagates publishing failure`() {
        val failingPort = PublishClickPort { throw ClickPublishingException("kafka down") }
        val service = RegisterClickService(failingPort, clock, idGenerator)

        assertThrows<ClickPublishingException> { service.register("ad-42") }
    }
}
