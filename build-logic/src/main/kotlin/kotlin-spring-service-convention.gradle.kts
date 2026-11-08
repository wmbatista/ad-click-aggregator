// Convention for runnable Spring Boot services under services/.
plugins {
    id("kotlin-service-convention")
    id("org.springframework.boot")
    id("org.jetbrains.kotlin.plugin.spring")
}

val libs = versionCatalogs.named("libs")

dependencies {
    implementation(platform(libs.findLibrary("spring-boot-dependencies").get()))
    testImplementation(libs.findLibrary("spring-boot-starter-test").get())
    testImplementation(libs.findLibrary("mockk").get())
}
