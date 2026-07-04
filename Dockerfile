# Build stage
FROM gradle:8.8-jdk17 AS builder
WORKDIR /build
COPY . .
RUN gradle clean jar --no-daemon

# Runtime stage
FROM eclipse-temurin:17-jre-jammy
WORKDIR /app

COPY --from=builder /build/build/libs/streamforge.jar /app/streamforge.jar
COPY --from=builder /build/streamforge.json /app/streamforge.json

RUN useradd -r -u 1000 appuser && chown -R appuser /app
USER appuser

# Args are the job name, e.g. `docker run <image> MongoToKafka`; no arg lists jobs.
ENTRYPOINT ["java", "-cp", "/app/streamforge.jar", "com.streamforge.core.launcher.Launcher"]
