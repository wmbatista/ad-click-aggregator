package com.wmbatista.adclick.queryservice.adapter.inbound.web

import com.wmbatista.adclick.queryservice.application.port.outbound.ClickStoreUnavailableException
import com.wmbatista.adclick.queryservice.domain.InvalidClickQueryException
import org.slf4j.LoggerFactory
import org.springframework.http.HttpStatus
import org.springframework.http.ProblemDetail
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.ExceptionHandler
import org.springframework.web.bind.annotation.RestControllerAdvice

@RestControllerAdvice
class ClickMetricsExceptionHandler {

    private val log = LoggerFactory.getLogger(javaClass)

    @ExceptionHandler(InvalidClickQueryException::class)
    fun handleInvalidQuery(e: InvalidClickQueryException): ResponseEntity<ProblemDetail> =
        ResponseEntity.badRequest().body(ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, e.message))

    /** ClickHouse could not be read; 503 tells the caller it is safe to retry. */
    @ExceptionHandler(ClickStoreUnavailableException::class)
    fun handleStoreUnavailable(e: ClickStoreUnavailableException): ResponseEntity<Void> {
        log.warn("Click store unavailable: {}", e.message, e)
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).build()
    }
}
