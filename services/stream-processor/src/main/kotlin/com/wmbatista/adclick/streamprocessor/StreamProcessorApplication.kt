package com.wmbatista.adclick.streamprocessor

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.ConfigurationPropertiesScan
import org.springframework.boot.runApplication

@SpringBootApplication
@ConfigurationPropertiesScan
class StreamProcessorApplication

fun main(args: Array<String>) {
    runApplication<StreamProcessorApplication>(*args)
}
