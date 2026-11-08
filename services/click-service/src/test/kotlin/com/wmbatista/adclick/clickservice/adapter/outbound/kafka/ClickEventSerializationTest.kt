package com.wmbatista.adclick.clickservice.adapter.outbound.kafka

import com.wmbatista.adclick.event.ClickEvent
import org.apache.kafka.common.header.internals.RecordHeaders
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test
import org.springframework.kafka.support.serializer.JacksonJsonSerializer
import tools.jackson.databind.json.JsonMapper
import java.time.Instant
import java.util.UUID

/** Pins the wire format produced by the value serializer configured in application.yml. */
class ClickEventSerializationTest {

    @Test
    fun `click event serializes as iso timestamp and uuid string without type headers`() {
        val serializer = JacksonJsonSerializer<ClickEvent>()
        serializer.configure(mapOf("spring.json.add.type.headers" to false), false)
        val headers = RecordHeaders()
        val event = ClickEvent(
            clickId = UUID.fromString("00000000-0000-0000-0000-000000000001"),
            adId = "ad-42",
            timestamp = Instant.parse("2026-11-06T12:34:56Z"),
        )

        val json = JsonMapper.builder().build().readTree(serializer.serialize("click-events", headers, event))

        assertEquals("2026-11-06T12:34:56Z", json.get("timestamp").asString())
        assertEquals("00000000-0000-0000-0000-000000000001", json.get("clickId").asString())
        assertEquals("ad-42", json.get("adId").asString())
        assertNull(headers.lastHeader("__TypeId__"))
    }
}
