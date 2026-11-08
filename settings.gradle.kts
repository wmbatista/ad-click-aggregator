pluginManagement {
    includeBuild("build-logic")
}

plugins {
    id("org.gradle.toolchains.foojay-resolver-convention") version "1.0.0"
}

dependencyResolutionManagement {
    repositories {
        mavenCentral()
    }
}

rootProject.name = "ad-click-aggregator"

include(
    "libs:click-event",
    "services:click-service",
    "services:stream-processor",
    "services:query-service",
)
