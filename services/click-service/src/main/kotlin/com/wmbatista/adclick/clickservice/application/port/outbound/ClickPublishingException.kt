package com.wmbatista.adclick.clickservice.application.port.outbound

/** Thrown when a click could not be durably published. */
class ClickPublishingException(message: String, cause: Throwable? = null) : RuntimeException(message, cause)
