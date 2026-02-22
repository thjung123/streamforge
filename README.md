# StreamForge

Pattern-composable streaming pipeline framework built on **Apache Flink 1.20.0**.

---

## Overview

StreamForge is a framework for building streaming pipelines by composing **reusable streaming patterns**. Each pipeline is a `StreamJob` that chains a source, parser, patterns, processor, and sink through the fluent `PipelineBuilder` API.

```java
PipelineBuilder
    .from(source.build(env, jobName))
    .parse(parser)
    .apply(patternA)
    .apply(patternB)
    .process(processor)
    .to(sink, jobName);
```

**Real example** — `MongoToKafkaJob` composes 8 patterns into a CDC pipeline:

```java
PipelineBuilder
    .from(new MultiCdcSourceBuilder().build(env, name()))
    .parse(new MongoToKafkaParser())
    .apply(new FlowDisruptionDetector<>(name()))
    .apply(new FilterInterceptor<>())
    .apply(new Deduplicator<>())
    .apply(new StatefulMerger<>())
    .apply(new SchemaEnforcer<>())
    .apply(new LatencyDetector<>(name()))
    .apply(new OnlineObserver<>(name()))
    .apply(new MetadataDecorator<>())
    .process(new MongoToKafkaProcessor())
    .to(KafkaSinkBuilder.exactlyOnce("txn-MongoToKafka"), name());
```

---

## Architecture

```
com.streamforge/
├── core/                          Framework engine
│   ├── config/                    ScopedConfig, ErrorCodes, ConfigKeys, MetricKeys
│   ├── dlq/                       DLQPublisher, DlqEvent
│   ├── launcher/                  StreamJob interface, JobLauncher
│   ├── metric/                    Metrics utilities
│   ├── model/                     StreamEnvelop, domain models
│   ├── parser/                    StreamEnvelopParser
│   ├── pipeline/                  PipelineBuilder + nested interfaces
│   └── util/                      JsonUtils, helpers
│
├── pattern/                       Reusable pattern blocks
│   ├── dedup/                     Deduplicator
│   ├── enrich/                    AsyncEnricher, DynamicJoiner, StaticJoiner
│   ├── filter/                    FilterInterceptor
│   ├── materialization/           Materializer
│   ├── merge/                     StatefulMerger
│   ├── observability/             FlowDisruptionDetector, LateDataDetector,
│   │                              LatencyDetector, MetadataDecorator, OnlineObserver
│   ├── quality/                   ConstraintEnforcer + rules/
│   ├── schema/                    SchemaEnforcer
│   ├── session/                   SessionAnalyzer
│   └── split/                     ParallelSplitter, OrderedFanIn
│
├── connector/                     Source/Sink adapters
│   ├── kafka/                     KafkaSourceBuilder, KafkaSinkBuilder
│   ├── mongo/                     MongoSourceBuilder, MongoChangeStreamSource,
│   │                              MultiCdcSourceBuilder, MongoSinkBuilder + util/
│   └── elasticsearch/             ElasticsearchSinkBuilder
│
└── job/                           Pipeline definitions = pattern compositions
    ├── cdc/                       MongoToKafkaJob, KafkaToMongoJob + parser/ + processor/
    ├── ingest/                    MergedIngestJob
    ├── join/                      OrderPaymentJoinJob
    ├── materialize/               UserStateMaterializeJob
    ├── route/                     EventRouterJob
    └── session/                   UserSessionAnalysisJob
```

### Core Interfaces

All defined as nested interfaces in `PipelineBuilder`:

- **`StreamPattern<T>`** — Common contract for type-preserving transformations. Implements `apply(DataStream<T>)`.
- **`JoinPattern<T, R>`** — Two-stream join contract. Implements `join(DataStream<T>, DataStream<R>)`.
- **`PipelineBuilder`** — Fluent API: `.from()` → `.parse()` → `.apply()` / `.enrich()` → `.process()` → `.to()`.
- **`StreamJob`** — Job contract with `name()` and `run()`. Each job has its own `main()` for Flink cluster submission.
- **`SinkBuilder<T>`** — Functional interface for sink creation: `write(DataStream<T>, String jobName)`.
- **`SourceBuilder<T>`** — Interface for source creation: `build(StreamExecutionEnvironment, String jobName)`.

### Extension Points

| Layer | How to Add | API |
|-------|-----------|-----|
| `pattern/` | Folder + `StreamPattern<T>` impl | `.apply(pattern)` |
| `pattern/` | Folder + `JoinPattern<T, R>` impl | `.enrich(refStream, pattern)` |
| `connector/` | Folder + `SourceBuilder<T>` impl | `.from(source.build(env, name))` |
| `connector/` | Folder + `SinkBuilder<T>` impl | `.to(sink, name)` |
| `job/` | Folder + `StreamJob` impl | Pipeline as pattern composition |

---

## Pattern Catalog

### Data Integrity (4)

| Pattern | Description | State |
|---------|-------------|-------|
| **Deduplicator** | Drops duplicate events by key within a TTL window | `ValueState<Boolean>` + TTL |
| **SchemaEnforcer** | Validates events against expected schema version; DLQ on mismatch | Stateless |
| **ConstraintEnforcer** | Validates business rules; DLQ on violation | Stateless |
| **StatefulMerger** | Emits only changed fields by diffing against previous record per key | `ValueState` (previous record) |

### Enrichment (3)

| Pattern | Description | State |
|---------|-------------|-------|
| **AsyncEnricher** | Async external lookups (Redis, HTTP) for stream enrichment | Stateless (async I/O) |
| **DynamicJoiner** | Two-stream keyed join with configurable TTL and join type | `MapState` x2 + `ValueState` + TTL |
| **StaticJoiner** | Broadcast join — enriches main stream with slowly-changing reference data | `BroadcastState` |

### Flow Control (3)

| Pattern | Description | State |
|---------|-------------|-------|
| **FilterInterceptor** | Configurable predicate-based event filtering | Stateless |
| **ParallelSplitter** | Splits stream into parallel side outputs by routing rules | Stateless |
| **OrderedFanIn** | Merges multiple streams by event time with configurable max drift | Stateless (event-time union) |

### Observability (5)

| Pattern | Description | State |
|---------|-------------|-------|
| **FlowDisruptionDetector** | Detects stream silence per key; emits disruption/recovery alerts | `ValueState<Boolean>` + `ValueState<Long>` |
| **LateDataDetector** | Detects and tags late-arriving events relative to watermark | Stateless |
| **LatencyDetector** | Measures end-to-end event latency; emits metrics | Stateless |
| **MetadataDecorator** | Adds traceId, timestamps, job metadata to events | Stateless |
| **OnlineObserver** | Emits throughput and health metrics per operator | Stateless |

### Stateful Processing (2)

| Pattern | Description | State |
|---------|-------------|-------|
| **Materializer** | Maintains latest value per key as a materialized table | `ValueState` (latest per key) |
| **SessionAnalyzer** | Groups events into sessions by configurable inactivity gap | Session window state |

---

## Jobs

| Job | Source | Pipeline | Sink |
|-----|--------|----------|------|
| **MongoToKafkaJob** | `MultiCdcSourceBuilder` (MongoDB CDC) | FlowDisruptionDetector → FilterInterceptor → Deduplicator → StatefulMerger → SchemaEnforcer → LatencyDetector → OnlineObserver → MetadataDecorator → MongoToKafkaProcessor | `KafkaSinkBuilder.exactlyOnce()` |
| **KafkaToMongoJob** | `KafkaSourceBuilder` | Optional `StaticJoiner` x2 (reference topics) → KafkaToMongoProcessor | `MongoSinkBuilder` |
| **MergedIngestJob** | `KafkaSourceBuilder` x2 | `OrderedFanIn` (event-time merge, 5s max drift) → StreamEnvelopParser | `MongoSinkBuilder` |
| **OrderPaymentJoinJob** | `KafkaSourceBuilder` x2 | `DynamicJoiner` (configurable TTL, default 10 min) | `MongoSinkBuilder` |
| **UserStateMaterializeJob** | `KafkaSourceBuilder` | `Materializer` (latest value per key) | `KafkaSinkBuilder.compacted()` |
| **EventRouterJob** | `KafkaSourceBuilder` | `ParallelSplitter` (orders/payments side outputs) | `ElasticsearchSinkBuilder` + `MongoSinkBuilder` |
| **UserSessionAnalysisJob** | `KafkaSourceBuilder` | `SessionAnalyzer` (configurable gap, default 1800s) | `MongoSinkBuilder` |

---

## Connectors

### Sources

| Connector | Type | Output | Notes |
|-----------|------|--------|-------|
| **KafkaSourceBuilder** | Unbounded | `String` | Flink-managed consumer offsets; group ID `"stream-group"` |
| **MongoSourceBuilder** | Bounded | `Document` | Batch/bounded MongoDB read |
| **MongoChangeStreamSource** | Unbounded | `Document` | Custom CDC source; resume tokens **NOT** checkpointed (see [Known Limitations](docs/recovery/fault-tolerance.md#7-known-limitations)) |
| **MultiCdcSourceBuilder** | Unbounded | `Document` | N parallel `MongoChangeStreamSource` instances with hash-mod split; default `CDC_PARALLELISM=4` |

### Sinks

| Connector | Delivery Guarantee | Notes |
|-----------|--------------------|-------|
| **KafkaSinkBuilder** | AT_LEAST_ONCE (default), EXACTLY_ONCE (`.exactlyOnce(prefix)`), compacted (`.compacted()`), compacted+exactly-once (`.compactedExactlyOnce(prefix)`) | Tombstone records on DELETE when compacted |
| **MongoSinkBuilder** | At-least-once with idempotent `replaceOne` by `_id` | Natural deduplication on retry |
| **ElasticsearchSinkBuilder** | At-least-once with idempotent index by `traceId` | Elasticsearch 7 |

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
    "MONGO_COLLECTION": "orders",
    "CDC_PARALLELISM": "4"
  },
  "KafkaToMongo": {
    "STREAM_TOPIC": "mongo-events",
    "MONGO_DB": "sink_db",
    "MONGO_COLLECTION": "orders_mirror",
    "REFERENCE_TOPIC": "ref-topic-1",
    "REFERENCE_TOPIC_2": "ref-topic-2"
  },
  "MergedIngest": {
    "STREAM_TOPIC": "primary-events",
    "SECONDARY_TOPIC": "secondary-events",
    "MONGO_DB": "ingest_db",
    "MONGO_COLLECTION": "merged"
  },
  "OrderPaymentJoin": {
    "ORDER_TOPIC": "orders",
    "PAYMENT_TOPIC": "payments",
    "JOIN_TTL_MINUTES": "10",
    "MONGO_DB": "join_db",
    "MONGO_COLLECTION": "order_payments"
  },
  "UserStateMaterialize": {
    "STREAM_TOPIC": "user-events",
    "CHANGELOG_TOPIC": "user-state-changelog"
  },
  "EventRouter": {
    "STREAM_TOPIC": "all-events",
    "ES_INDEX": "events",
    "MONGO_DB": "route_db",
    "MONGO_COLLECTION": "routed_events"
  },
  "UserSessionAnalysis": {
    "STREAM_TOPIC": "user-activity",
    "SESSION_GAP_SECONDS": "1800",
    "MONGO_DB": "session_db",
    "MONGO_COLLECTION": "sessions"
  }
}
```

### Production

In production, use environment variables, JVM system properties, or Vault/K8s Secrets. These always take priority over `.env` and `streamforge.json`.

---

## Getting Started

### Prerequisites

- Java 17+
- Docker & Docker Compose (for local infrastructure)
- Gradle (wrapper included)

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

---

## Testing

### Unit Tests

```bash
./gradlew test
```

Unit tests cover individual patterns, parsers, processors, and utility classes using JUnit 5, Mockito, and AssertJ.

### Integration Tests

```bash
./gradlew integrationTest
```

Integration tests use **Testcontainers** to spin up real Kafka, MongoDB, and Elasticsearch instances. Tests validate end-to-end pipeline behavior including serialization, DLQ routing, and sink writes.

---

## CI

GitHub Actions runs on every push to `main` and on pull requests targeting `main`:

| Step | Command |
|------|---------|
| Build | `./gradlew clean assemble` |
| Integration Tests | `./gradlew integrationTest` |
| Artifact Upload | `build/test-results/integrationTest` |

Runner: `ubuntu-latest` with JDK 17 (Temurin). Docker is available for Testcontainers.

---

## Tech Stack

| Component | Version |
|-----------|---------|
| **Apache Flink** | 1.20.0 |
| **flink-connector-kafka** | 3.3.0-1.20 |
| **flink-connector-mongodb** | 1.2.0-1.18 |
| **flink-connector-elasticsearch7** | 3.1.0-1.18 |
| **Apache Kafka** | Event bus, DLQ |
| **MongoDB** | Source/Sink |
| **Elasticsearch** | 7.17.x (via connector) |
| **Redis** | Lettuce 6.3.2 / Jedis 5.1.0 |
| **Jackson** | 2.18.2 |
| **Java** | 17 |
| **Gradle** | Build (wrapper included) |
| **Testcontainers** | 1.21.4 |
| **Error Prone** | 2.36.0 |
| **Spotless** | 7.0.2 (Google Java Format) |
| **Docker Compose** | Local infrastructure |

---

## Project Structure

```
streamforge/
├── build.gradle.kts           Build configuration
├── settings.gradle.kts        Project settings
├── gradle.properties          Gradle properties
├── docker-compose.yml         Local infrastructure (Kafka, MongoDB, ES)
├── Dockerfile                 Container build
├── streamforge.json           Job-specific configuration
├── .env                       Local environment variables
├── src/
│   ├── main/java/com/streamforge/   Application source
│   └── test/java/com/streamforge/   Unit + integration tests
├── docs/                      Documentation
│   └── recovery/              Fault tolerance, checkpoints, DLQ
├── scripts/                   Operational scripts
├── local_test/                Local test utilities
└── .github/workflows/ci.yml  CI pipeline
```
