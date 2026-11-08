package com.wmbatista.adclick.clickservice

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.context.properties.ConfigurationPropertiesScan
import org.springframework.boot.runApplication

@SpringBootApplication
@ConfigurationPropertiesScan
class ClickServiceApplication

fun main(args: Array<String>) {
    runApplication<ClickServiceApplication>(*args)
}
