// Shared configuration for every Kotlin module in the repository.
plugins {
    id("org.jetbrains.kotlin.jvm")
}

val libs = versionCatalogs.named("libs")

kotlin {
    jvmToolchain(25)
}

dependencies {
    testImplementation(platform(libs.findLibrary("junit-bom").get()))
    testImplementation(libs.findLibrary("junit-jupiter").get())
    testRuntimeOnly(libs.findLibrary("junit-platform-launcher").get())
}

tasks.withType<Test>().configureEach {
    useJUnitPlatform()
}
