plugins {
    id("kotlin-service-convention")
}

dependencies {
    implementation(project(":libs:click-event"))
}

application {
    mainClass = "com.wmbatista.adclick.clickprocessor.MainKt"
}
