package com.wmbatista.adclick.queryservice.config

import com.wmbatista.adclick.queryservice.adapter.outbound.clickhouse.ClickHouseClickCountsAdapter
import com.wmbatista.adclick.queryservice.application.port.inbound.GetClickMetricsUseCase
import com.wmbatista.adclick.queryservice.application.port.outbound.LoadClickCountsPort
import com.wmbatista.adclick.queryservice.application.service.GetClickMetricsService
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.jdbc.core.JdbcTemplate
import java.time.Duration

@Configuration
class QueryServiceConfiguration {

    @Bean
    fun loadClickCountsPort(
        jdbcTemplate: JdbcTemplate,
        @Value("\${query-service.clickhouse.read-timeout}") readTimeout: Duration,
    ): LoadClickCountsPort = ClickHouseClickCountsAdapter(jdbcTemplate, readTimeout)

    @Bean
    fun getClickMetricsUseCase(loadClickCountsPort: LoadClickCountsPort): GetClickMetricsUseCase =
        GetClickMetricsService(loadClickCountsPort)
}
