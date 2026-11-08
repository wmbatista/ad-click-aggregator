package com.wmbatista.adclick.clickservice.adapter.inbound.web

import com.wmbatista.adclick.clickservice.application.port.outbound.ClickPublishingException
import org.slf4j.LoggerFactory
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.ExceptionHandler
import org.springframework.web.bind.annotation.RestControllerAdvice

@RestControllerAdvice
class ClickExceptionHandler {

    private val log = LoggerFactory.getLogger(javaClass)

    /** The click was not stored; 503 tells the caller it is safe to retry. */
    @ExceptionHandler(ClickPublishingException::class)
    fun handlePublishingFailure(e: ClickPublishingException): ResponseEntity<Void> {
        log.warn("Click could not be published: {}", e.message, e)
        return ResponseEntity.status(503).build()
    }
}
