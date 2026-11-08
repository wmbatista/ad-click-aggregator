package com.wmbatista.adclick.clickservice.adapter.inbound.web

import jakarta.validation.constraints.NotBlank

/** Body of `POST /click`. Nullable so a missing `adId` reaches validation and becomes a 400. */
data class ClickRequest(
    @field:NotBlank
    val adId: String?,
)
