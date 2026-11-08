package com.wmbatista.adclick.clickservice.application.service

import com.wmbatista.adclick.clickservice.application.port.inbound.RegisterClickUseCase
import com.wmbatista.adclick.clickservice.application.port.outbound.ClickIdGenerator
import com.wmbatista.adclick.clickservice.application.port.outbound.PublishClickPort
import com.wmbatista.adclick.clickservice.domain.Click
import java.time.Clock

class RegisterClickService(
    private val publishClickPort: PublishClickPort,
    private val clock: Clock,
    private val clickIdGenerator: ClickIdGenerator,
) : RegisterClickUseCase {

    override fun register(adId: String): Click {
        val click = Click(clickIdGenerator.next(), adId, clock.instant())
        publishClickPort.publish(click)
        return click
    }
}
