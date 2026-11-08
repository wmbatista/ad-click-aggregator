plugins {
    id("kotlin-spring-service-convention")
}

dependencies {
    implementation(libs.spring.boot.starter.webmvc)
    implementation(libs.spring.boot.starter.jdbc)
    implementation(libs.spring.boot.starter.actuator)
    implementation(libs.jackson.module.kotlin)
    runtimeOnly(libs.clickhouse.jdbc)

    testImplementation(libs.spring.boot.testcontainers)
    testImplementation(libs.testcontainers.junit.jupiter)
    testImplementation(libs.testcontainers.clickhouse)
}

application {
    mainClass = "com.wmbatista.adclick.queryservice.QueryServiceApplicationKt"
}

tasks.test {
    // Tests create the table from the same DDL that Compose mounts into ClickHouse.
    systemProperty("clickhouse.schema", rootProject.file("infra/clickhouse/init/001-schema.sql").absolutePath)
}
