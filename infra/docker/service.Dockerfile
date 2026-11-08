# Packages any service jar built on the host (./gradlew bootJar).
# Build context is the repository root; pass the jar path relative to it as JAR_FILE.
FROM eclipse-temurin:25-jre

ARG JAR_FILE

# curl is not in the base image; Compose healthchecks call /actuator/health with it.
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

RUN groupadd --system app && useradd --system --gid app app
WORKDIR /app
COPY ${JAR_FILE} /app/app.jar

USER app
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
