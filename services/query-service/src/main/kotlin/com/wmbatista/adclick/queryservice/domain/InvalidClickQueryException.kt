package com.wmbatista.adclick.queryservice.domain

/** The requested query cannot be answered; the message explains why and is shown to the caller. */
class InvalidClickQueryException(message: String) : RuntimeException(message)
