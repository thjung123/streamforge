# Build stage
FROM gradle:8.8-jdk17 AS builder
WORKDIR /build
COPY . .
RUN gradle clean jar --no-daemon

FROM openjdk:17-jdk-slim
WORKDIR /app

COPY --from=builder /build/build/libs/streamforge.jar /app/streamforge.jar
COPY .env /app/.env


ENTRYPOINT ["java", "-cp", "/app/streamforge.jar"]
