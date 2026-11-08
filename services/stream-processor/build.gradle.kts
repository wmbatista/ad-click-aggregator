plugins {
    id("kotlin-spring-service-convention")
}

dependencies {
    implementation(project(":libs:click-event"))
    // Web server only to expose Actuator health over HTTP; there is no business HTTP API.
    implementation(libs.spring.boot.starter.webmvc)
    implementation(libs.spring.boot.starter.kafka)
    implementation(libs.spring.boot.starter.jdbc)
    implementation(libs.spring.boot.starter.actuator)
    implementation(libs.jackson.module.kotlin)
    runtimeOnly(libs.clickhouse.jdbc)

    testImplementation(libs.spring.boot.testcontainers)
    testImplementation(libs.testcontainers.junit.jupiter)
    testImplementation(libs.testcontainers.clickhouse)
    testImplementation(libs.testcontainers.kafka)
}

application {
    mainClass = "com.wmbatista.adclick.streamprocessor.StreamProcessorApplicationKt"
}

tasks.test {
    // A non-UTC zone makes any accidental use of the JVM default zone shift the minute buckets.
    systemProperty("user.timezone", "America/Sao_Paulo")
    // Tests create the table from the same DDL Compose mounts into ClickHouse.
    systemProperty("clickhouse.schema", rootProject.file("infra/clickhouse/init/001-schema.sql").absolutePath)
}
