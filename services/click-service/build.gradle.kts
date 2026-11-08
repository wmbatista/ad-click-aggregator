plugins {
    id("kotlin-spring-service-convention")
}

dependencies {
    implementation(project(":libs:click-event"))
    implementation(libs.spring.boot.starter.webmvc)
    implementation(libs.spring.boot.starter.kafka)
    implementation(libs.spring.boot.starter.validation)
    implementation(libs.jackson.module.kotlin)
}

application {
    mainClass = "com.wmbatista.adclick.clickservice.ClickServiceApplicationKt"
}
