package com.wmbatista.adclick.queryservice.application.port.outbound

/** The click store could not be read; the request may succeed if retried later. */
class ClickStoreUnavailableException(message: String, cause: Throwable? = null) : RuntimeException(message, cause)
