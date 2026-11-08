package com.wmbatista.adclick.clickservice.adapter.inbound.web

import com.wmbatista.adclick.clickservice.application.port.inbound.RegisterClickUseCase
import com.wmbatista.adclick.clickservice.application.port.outbound.ClickPublishingException
import com.wmbatista.adclick.clickservice.domain.Click
import io.mockk.every
import io.mockk.mockk
import io.mockk.verify
import org.junit.jupiter.api.Test
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.ValueSource
import org.springframework.http.MediaType
import org.springframework.test.web.servlet.MockMvc
import org.springframework.test.web.servlet.post
import org.springframework.test.web.servlet.setup.MockMvcBuilders
import java.time.Instant
import java.util.UUID

class ClickControllerTest {

    private val useCase = mockk<RegisterClickUseCase>()

    private val mockMvc: MockMvc = MockMvcBuilders
        .standaloneSetup(ClickController(useCase))
        .setControllerAdvice(ClickExceptionHandler())
        .build()

    private fun postClick(body: String) = mockMvc.post("/click") {
        contentType = MediaType.APPLICATION_JSON
        content = body
    }

    @Test
    fun `valid click returns 202`() {
        every { useCase.register("ad-42") } returns
            Click(UUID.randomUUID(), "ad-42", Instant.parse("2026-11-06T12:34:56Z"))

        postClick("""{"adId":"ad-42"}""").andExpect {
            status { isAccepted() }
            content { string("") }
        }

        verify(exactly = 1) { useCase.register("ad-42") }
    }

    @ParameterizedTest
    @ValueSource(strings = ["{}", """{"adId":null}""", """{"adId":""}""", """{"adId":"   "}"""])
    fun `missing, null, empty or blank adId returns 400`(body: String) {
        postClick(body).andExpect { status { isBadRequest() } }

        verify { useCase wasNot io.mockk.Called }
    }

    @Test
    fun `malformed body returns 400`() {
        postClick("not json").andExpect { status { isBadRequest() } }

        verify { useCase wasNot io.mockk.Called }
    }

    @Test
    fun `publishing failure returns 503`() {
        every { useCase.register("ad-42") } throws ClickPublishingException("down")

        postClick("""{"adId":"ad-42"}""").andExpect { status { isServiceUnavailable() } }
    }
}
