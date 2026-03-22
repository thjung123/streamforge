# StreamForge

**Pattern-composable streaming pipeline framework built on Apache Flink 1.20.0**

---

## TL;DR

- Composable streaming framework that turns Flink pipelines into **chains of reusable pattern blocks**
- **17 patterns** across 5 categories: data integrity, enrichment, flow control, observability, stateful processing
- Production-ready jobs covering CDC sync, stream joins, materialized views, event routing, session analytics
- Pluggable connectors (Kafka, MongoDB, Elasticsearch) with configurable delivery guarantees

---

## Why This Exists

Real-world Flink pipelines share the same cross-cutting concerns — deduplication, schema validation, latency monitoring, DLQ routing — but these get copy-pasted and re-implemented across jobs. StreamForge extracts them into **composable, testable pattern blocks** that snap together through a fluent API.

**The core idea:** a streaming pipeline is a composition of patterns, not a monolithic `ProcessFunction`.

```
Source → Parse → [Pattern₁ → Pattern₂ → ... → Patternₙ] → Process → Sink
```

This means:
- **Adding observability** to any pipeline = `.apply(new LatencyDetector<>())` — one line, not a refactor
- **Each pattern is independently testable** with its own unit tests, no pipeline wiring needed
- **New jobs are just new compositions** — the MongoToKafkaJob chains 8 patterns in 12 lines of code

---

## Architecture

### Pipeline Composition Model

Every pipeline is built through `PipelineBuilder`, a fluent API that enforces a consistent structure:

```java
PipelineBuilder
    .from(source)          // SourceBuilder<T> — where data comes from
    .parse(parser)         // ParserFunction<I,O> — raw → domain type
    .apply(pattern)        // StreamPattern<T> — reusable transformation (chainable)
    .enrich(ref, joiner)   // JoinPattern<T,R> — two-stream enrichment
    .process(processor)    // ProcessorFunction<I,O> — final business logic
    .to(sink, jobName);    // SinkBuilder<T> — where data goes
```

### Real Example — MongoToKafkaJob

This CDC pipeline chains 8 patterns to go from MongoDB change streams to Kafka with exactly-once delivery:

```java
PipelineBuilder
    .from(new MultiCdcSourceBuilder().build(env, name()))    // 4-way parallel CDC
    .parse(new MongoToKafkaParser())                         // Document → StreamEnvelop
    .apply(new FlowDisruptionDetector<>(name()))              // Alert on stream silence
    .apply(new FilterInterceptor<>())                        // Drop irrelevant events
    .apply(new Deduplicator<>())                             // TTL-based dedup by key
    .apply(new StatefulMerger<>())                           // Emit only changed fields
    .apply(new SchemaEnforcer<>())                           // Validate + DLQ on mismatch
    .apply(new LatencyDetector<>(name()))                    // Track end-to-end latency
    .apply(new OnlineObserver<>(name()))                     // Throughput + health metrics
    .apply(new MetadataDecorator<>())                        // Inject traceId + timestamps
    .process(new MongoToKafkaProcessor())                    // Final transformation
    .to(KafkaSinkBuilder.exactlyOnce("txn-MongoToKafka"), name());
```

**Why this composition matters:** each pattern handles one concern. Removing `SchemaEnforcer` doesn't break dedup. Adding `LateDataDetector` doesn't require touching existing logic. The pipeline reads like a specification.

### Core Abstractions

| Interface | Role | Method |
|-----------|------|--------|
| `StreamPattern<T>` | Single-stream, type-preserving transformation | `apply(DataStream<T>)` |
| `JoinPattern<T, R>` | Two-stream join | `join(DataStream<T>, DataStream<R>)` |
| `StreamJob` | Job contract with SPI registration | `name()`, `run(args)` |
| `SourceBuilder<T>` | Source factory | `build(env, jobName)` |
| `SinkBuilder<T>` | Sink factory | `write(stream, jobName)` |

```
com.streamforge/
├── core/          Framework engine (PipelineBuilder, ScopedConfig, DLQ, metrics)
├── pattern/       17 reusable pattern blocks
├── connector/     Source/Sink adapters (Kafka, MongoDB, Elasticsearch)
└── job/           7 pipeline definitions = pattern compositions
```

---

## Pattern Catalog (17 patterns)

### Data Integrity

| Pattern | What It Does | State | Key Design Decision |
|---------|-------------|-------|---------------------|
| **Deduplicator** | Drops duplicates by key within TTL window | `ValueState<Boolean>` + TTL | TTL-based eviction avoids unbounded state growth |
| **SchemaEnforcer** | Validates schema version; routes violations to DLQ | Stateless | Fail-fast with DLQ fallback — bad data never reaches the sink |
| **ConstraintEnforcer** | Validates business rules via pluggable `ConstraintRule<T>` | Stateless | Rule interface (`NotNull`, `Range`, `Format`) for extensibility |
| **StatefulMerger** | Emits only changed fields by diffing against previous record | `ValueState` (hash) | Hash comparison, not deep-equals — O(1) per event |

### Enrichment

| Pattern | What It Does | State | Key Design Decision |
|---------|-------------|-------|---------------------|
| **AsyncEnricher** | Async external lookups (Redis, HTTP) | Async I/O | Flink's `AsyncDataStream` for non-blocking enrichment |
| **DynamicJoiner** | Two-stream keyed join with configurable TTL | `MapState` x2 + TTL | Supports INNER/LEFT/RIGHT/FULL_OUTER join types |
| **StaticJoiner** | Broadcast join with slowly-changing reference data | `BroadcastState` | Broadcast pattern — reference data replicated to all operators |

### Flow Control

| Pattern | What It Does | State | Key Design Decision |
|---------|-------------|-------|---------------------|
| **FilterInterceptor** | Predicate-based filtering | Stateless | Composable predicates, not hardcoded conditions |
| **ParallelSplitter** | Routes events to named side outputs | Stateless | Flink `OutputTag` for zero-copy fan-out |
| **OrderedFanIn** | Merges streams by event time with max drift bound | Stateless | Bounded out-of-orderness prevents unbounded buffering |

### Observability

| Pattern | What It Does | State | Key Design Decision |
|---------|-------------|-------|---------------------|
| **FlowDisruptionDetector** | Detects stream silence; emits disruption/recovery alerts | `ValueState` x2 + timers | Processing-time timers — works even when event flow stops |
| **LateDataDetector** | Tags late-arriving events relative to watermark | Stateless | Detection only, not correction — downstream decides policy |
| **LatencyDetector** | Measures end-to-end latency; alerts on threshold breach | Stateless | `processingTime - eventTime` as the latency signal |
| **MetadataDecorator** | Injects traceId, timestamps, job metadata | Stateless | Enables distributed tracing across pipeline stages |
| **OnlineObserver** | Emits throughput and health metrics via custom predicates | Stateless | Pluggable `QualityCheck` — define what "healthy" means per job |

### Stateful Processing

| Pattern | What It Does | State | Key Design Decision |
|---------|-------------|-------|---------------------|
| **Materializer** | Maintains latest value per key; outputs changelog (before/after) | `ValueState` | `ChangelogEvent<T>` captures state transitions for downstream |
| **SessionAnalyzer** | Groups events into sessions by inactivity gap | Session window | Configurable gap + allowed lateness + out-of-orderness bounds |

---

## Jobs

Each job is a **composition of patterns** — the job class itself is typically 20-40 lines.

| Job | What It Does | Patterns Used | Source → Sink |
|-----|-------------|---------------|---------------|
| **MongoToKafkaJob** | CDC sync from MongoDB to Kafka | 8 patterns (dedup, schema, merge, observability) | MongoDB CDC → Kafka (exactly-once) |
| **KafkaToMongoJob** | Reverse CDC with optional reference enrichment | StaticJoiner x2 (optional) | Kafka → MongoDB (idempotent upsert) |
| **MergedIngestJob** | Merge two event streams by event time | OrderedFanIn (5s max drift) | Kafka x2 → MongoDB |
| **OrderPaymentJoinJob** | Join orders with payments within time window | DynamicJoiner (10min TTL, LEFT join) | Kafka x2 → MongoDB |
| **UserStateMaterializeJob** | Maintain latest user state as changelog | Materializer | Kafka → Kafka (compacted) |
| **EventRouterJob** | Route events to different sinks by type | ParallelSplitter | Kafka → Elasticsearch + MongoDB |
| **UserSessionAnalysisJob** | Compute user sessions from activity events | SessionAnalyzer (30min gap) | Kafka → MongoDB |

---

## Connectors

### Sources

| Connector | Bounded? | Key Detail |
|-----------|----------|------------|
| **KafkaSourceBuilder** | Unbounded | Flink-managed offsets; group `"stream-group"` |
| **MongoSourceBuilder** | Bounded | Batch reads from MongoDB |
| **MongoChangeStreamSource** | Unbounded | Custom CDC via change streams; auto-reconnect on failure |
| **MultiCdcSourceBuilder** | Unbounded | N parallel CDC instances with hash-mod key split (default N=4) |

### Sinks

| Connector | Delivery Guarantee | Key Detail |
|-----------|--------------------|------------|
| **KafkaSinkBuilder** | AT_LEAST_ONCE / EXACTLY_ONCE / compacted | Tombstone on DELETE when compacted |
| **MongoSinkBuilder** | At-least-once | Idempotent `replaceOne` by `_id` — natural dedup |
| **ElasticsearchSinkBuilder** | At-least-once | Idempotent index by `traceId` |

---

## Error Handling & Fault Tolerance

### Dead Letter Queue (DLQ)

Failed events are routed to a Kafka DLQ topic with full context:

```json
{
  "errorCode": "SCHEMA_VIOLATION",
  "errorMessage": "Expected schema v2, got v1",
  "sourceOperator": "SchemaEnforcer",
  "originalPayload": "{ ... }",
  "stackTrace": "...",
  "timestamp": 1700000000000
}
```

Error classification: `PARSING_ERROR`, `PROCESSING_ERROR`, `SINK_ERROR`, `CONSTRAINT_VIOLATION`, `SCHEMA_VIOLATION`

DLQ itself is async fire-and-forget — DLQ failures don't block the pipeline. Sink-level delivery guarantees are documented in [Connectors](#sinks).

---

## Configuration

Hierarchical resolution (highest priority first):

```
System property → Environment variable → .env → streamforge.json[job] → streamforge.json[common]
```

Each job activates its scoped config at startup via `ScopedConfig.activateJob(name())`.

```json
{
  "common": {
    "KAFKA_BOOTSTRAP_SERVERS": "localhost:9092",
    "DLQ_TOPIC": "stream-dlq"
  },
  "MongoToKafka": {
    "STREAM_TOPIC": "mongo-events",
    "CDC_PARALLELISM": "4"
  }
}
```

---

## Testing

- **Unit tests** — Each pattern, parser, processor tested in isolation (JUnit 5 + Mockito + AssertJ). Patterns don't know which pipeline they belong to — a direct benefit of the composition model.
- **Integration tests** — Full pipeline tests with **Testcontainers** (real Kafka, MongoDB, Elasticsearch). Pushes events through real connectors and asserts on actual sink state.

```bash
./gradlew test                # Unit tests
./gradlew integrationTest     # Integration tests (requires Docker)
```

---

## Extension Points

| What to Add | Implement | Wire With |
|-------------|-----------|-----------|
| New pattern | `StreamPattern<T>` | `.apply(pattern)` |
| New join pattern | `JoinPattern<T, R>` | `.enrich(refStream, pattern)` |
| New source | `SourceBuilder<T>` | `.from(source.build(env, name))` |
| New sink | `SinkBuilder<T>` | `.to(sink, name)` |
| New job | `StreamJob` + SPI registration | Pattern composition via `PipelineBuilder` |

---

## Tech Stack

| | |
|---|---|
| **Runtime** | Java 17, Apache Flink 1.20.0 |
| **Connectors** | flink-connector-kafka 3.3.0, flink-connector-mongodb 1.2.0, flink-connector-elasticsearch7 3.1.0 |
| **External** | Redis (Lettuce / Jedis) |
| **Testing** | JUnit 5, Mockito, Testcontainers |
| **Code Quality** | Error Prone, Spotless (Google Java Format) |

---

## Getting Started

```bash
# Local infrastructure
docker-compose up -d

# Build
./gradlew jar

# Run a job
java -cp build/libs/streamforge.jar com.streamforge.job.cdc.MongoToKafkaJob

# Or submit to Flink cluster
flink run -c com.streamforge.job.cdc.MongoToKafkaJob streamforge.jar
```
