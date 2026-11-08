package com.wmbatista.adclick.clickservice.adapter.inbound.web

import com.wmbatista.adclick.clickservice.application.port.inbound.RegisterClickUseCase
import jakarta.validation.Valid
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RestController

@RestController
class ClickController(
    private val registerClickUseCase: RegisterClickUseCase,
) {

    /** Answers 202 once the click is durably published; aggregation happens asynchronously. */
    @PostMapping("/click")
    fun click(@Valid @RequestBody request: ClickRequest): ResponseEntity<Void> {
        registerClickUseCase.register(requireNotNull(request.adId))
        return ResponseEntity.accepted().build()
    }
}
