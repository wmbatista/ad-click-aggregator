package com.wmbatista.adclick.queryservice

import org.springframework.core.env.Environment
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration

/** GETs [path] from the service started by a `RANDOM_PORT` Spring Boot test. */
fun Environment.httpGet(path: String): HttpResponse<String> {
    val port = requireNotNull(getProperty("local.server.port"))
    val request = HttpRequest.newBuilder(URI.create("http://localhost:$port$path")).timeout(Duration.ofSeconds(30)).GET().build()
    return HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString())
}
