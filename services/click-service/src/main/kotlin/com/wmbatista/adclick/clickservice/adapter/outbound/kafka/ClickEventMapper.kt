package com.wmbatista.adclick.clickservice.adapter.outbound.kafka

import com.wmbatista.adclick.clickservice.domain.Click
import com.wmbatista.adclick.event.ClickEvent

fun Click.toClickEvent(): ClickEvent = ClickEvent(clickId = clickId, adId = adId, timestamp = timestamp)
