# StreamForge

Pattern-composable streaming pipeline framework built on Apache Flink.

---

## Overview

StreamForge is a framework for building streaming pipelines by composing **reusable streaming patterns**.

```java
PipelineBuilder
    .from(source.build(env, jobName))
    .parse(parser)
    .apply(patternA)
    .apply(patternB)
    .process(processor)
    .to(sink, jobName);
```

---

## Architecture

```
com.streamforge/
├── core/           Framework engine (PipelineBuilder, StreamPattern, models)
├── pattern/        Reusable pattern blocks (each implements StreamPattern)
├── connector/      Source/Sink adapters (Kafka, MongoDB, ...)
└── job/            Pipeline definitions = pattern compositions
```

### Core Interfaces

- **`StreamPattern<T>`** — Common contract for all patterns. Implements `apply(DataStream<T>)` to transform a stream.
- **`PipelineBuilder`** — Chains source, parser, patterns (`.apply()`), processor, and sink into a pipeline.
- **`StreamJob`** — Job contract with `name()` and `run()`. Each job has its own `main()` for Flink cluster submission.

### Extension Points

| Layer | How to Add | Result |
|-------|-----------|--------|
| `pattern/` | Folder + `StreamPattern` impl | Apply with `.apply()` |
| `connector/` | Folder + Source/SinkBuilder | Use via `.from()` / `.to()` |
| `job/` | Folder + `StreamJob` impl | Pipeline as pattern composition |

---

## Getting Started

### Local

```bash
docker-compose up -d
./gradlew jar
java -cp build/libs/streamforge.jar <job-class>
```

### Flink Cluster

```bash
flink run -c <job-class> streamforge.jar
```

### Docker

```bash
docker build -t streamforge .
docker run streamforge <job-class>
```

### Tests

```bash
./gradlew test                # unit tests
./gradlew integrationTest     # integration tests (Testcontainers)
```

---

## Configuration

StreamForge uses a **hierarchical configuration** system. Resolution order (highest priority first):

```
System property → env var → .env → streamforge.json[job] → streamforge.json[common]
```

### `.env` — Infrastructure commons

Shared settings that rarely change per job. Lives at project root.

```env
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
MONGO_URI=mongodb://localhost:27017/?replicaSet=rs0
APP_ENV=local
```

### `streamforge.json` — Job-specific + common defaults

Job-scoped settings and shared defaults. Each job calls `ScopedConfig.activateJob(name())` at startup.

```json
{
  "common": {
    "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
    "DLQ_TOPIC": "stream-dlq"
  },
  "MongoToKafka": {
    "STREAM_TOPIC": "mongo-events",
    "MONGO_DB": "source_db",
    "MONGO_COLLECTION": "orders"
  },
  "KafkaToMongo": {
    "STREAM_TOPIC": "mongo-events",
    "MONGO_DB": "sink_db",
    "MONGO_COLLECTION": "orders_mirror"
  }
}
```

### Production

In production, use environment variables, JVM system properties, or Vault/K8s Secrets. These always take priority over `.env` and `streamforge.json`.

---

## Tech Stack

- **Apache Flink 1.19** — Stream processing engine
- **Apache Kafka** — Event bus, DLQ
- **MongoDB** — Source/Sink
- **Java 17** — Language
- **Gradle** — Build
- **Testcontainers** — Integration tests
- **Docker Compose** — Local infrastructure
