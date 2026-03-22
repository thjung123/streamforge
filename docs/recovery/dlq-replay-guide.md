# DLQ Replay Guide

This document describes the **DLQ (Dead Letter Queue)** implementation in StreamForge and the replay procedure for failed events.

---

> **Status**
> - **DLQ publishing**: IMPLEMENTED — `DLQPublisher` routes failed events to Kafka DLQ topics across 9 publishing points.
> - **Airflow replay DAG**: DESIGN DOCUMENT — not yet built. See [Planned Design](#6-planned-design-airflow-replay-dag) below.
> - **Current workaround**: Manual replay via `kafka-console-consumer` / `kafka-console-producer` or custom scripts.

---

## 1. Purpose & Scope

The **Dead Letter Queue (DLQ)** stores events that failed during parsing, transformation, quality enforcement, or sink operations. It ensures data durability and allows reprocessing once the underlying issue has been fixed.

This guide covers:
- Current DLQ implementation (what is built today)
- DLQ event structure and error types
- All publishing points
- Manual replay procedure
- Planned Airflow-based replay workflow (not yet implemented)

---

## 2. Current Implementation

### DLQPublisher

`DLQPublisher` (`com.streamforge.core.dlq.DLQPublisher`) is a thread-safe singleton that publishes failed events to a Kafka DLQ topic.

- **Transport**: Standalone `KafkaProducer<String, String>` (not Flink's transactional sink).
- **Topic**: Configured via `DLQ_TOPIC` (default: `stream-dlq`).
- **Serialization**: `DlqEvent` → JSON via `JsonUtils.toJson()`.
- **Delivery**: Async send with callback. Non-transactional — best-effort delivery.
- **Metrics**: `dlq.published_count` on success, `dlq.failed_count` on failure.

### DlqEvent Structure

| Field | Type | Description |
|-------|------|-------------|
| `errorType` | `String` | Error category (see table below) |
| `errorMessage` | `String` | Exception message |
| `source` | `String` | Originating operator name |
| `timestamp` | `Instant` | Time of DLQ publish (`Instant.now()`) |
| `rawEvent` | `String` | Original JSON payload (nullable) |
| `stacktrace` | `String` | Full stack trace (nullable) |

Created via `DlqEvent.of(errorType, errorMessage, source, rawEvent, cause)`.

### Publishing Points

| # | Class | Error Type | Trigger |
|---|-------|------------|---------|
| 1 | `StreamEnvelopParser` | `PARSING_ERROR` | JSON deserialization failure |
| 2 | `MongoToKafkaParser` | `PARSING_ERROR` | CDC document parse failure |
| 3 | `MongoToKafkaProcessor` | `PROCESSING_ERROR` | Transformation logic error |
| 4 | `KafkaToMongoProcessor` | `PROCESSING_ERROR` | Transformation logic error |
| 5 | `MongoChangeStreamSource` | `SOURCE_PARSING_ERROR` | Exception reading change stream event |
| 6 | `MongoSinkBuilder` | `SINK_ERROR` | Exception writing to MongoDB |
| 7 | `KafkaSinkBuilder` | `SINK_ERROR` | Exception in pre-sink metrics map function |
| 8 | `ConstraintEnforcer` | `CONSTRAINT_VIOLATION` | Business rule validation failure |
| 9 | `SchemaEnforcer` | `SCHEMA_VIOLATION` | Schema version mismatch |

---

## 3. Failure Categories

| Error Type | Source | Description | Resolution |
|-----------|--------|-------------|------------|
| `SOURCE_PARSING_ERROR` | `MongoChangeStreamSource` | Failed to read a change stream event | Fix MongoDB configuration or schema, then replay |
| `PARSING_ERROR` | `StreamEnvelopParser`, `MongoToKafkaParser` | Failed to deserialize or parse input | Fix schema or input format, redeploy, then replay |
| `PROCESSING_ERROR` | `MongoToKafkaProcessor`, `KafkaToMongoProcessor` | Transformation or enrichment logic failed | Patch and redeploy pipeline, then replay |
| `SCHEMA_VIOLATION` | `SchemaEnforcer` | Event schema version does not match expected version | Update schema enforcement rules or fix upstream producer |
| `CONSTRAINT_VIOLATION` | `ConstraintEnforcer` | Business rule validation failed | Update constraint rules or fix upstream data |
| `SINK_ERROR` | `MongoSinkBuilder`, `KafkaSinkBuilder` | Failed to write to target | Fix connection/configuration, then replay |

---

## 4. DLQ Policy

- Each pipeline writes to a shared **Kafka DLQ topic** (configured via `DLQ_TOPIC`, default `stream-dlq`).
- Events in DLQ are **not reprocessed automatically**.
- Replay should only occur **after** the cause of failure has been identified and fixed.
- Retention is typically **7–14 days** to allow safe recovery without backlog growth.

---

## 5. Manual Replay (Current Workaround)

Since the automated replay DAG is not yet implemented, use this manual procedure:

### Prerequisites

1. The root cause has been identified and fixed (schema, code, sink config, etc.).
2. DLQ messages have been inspected and validated.
3. Downstream systems are healthy and can accept data.
4. Monitoring and alerting are active to observe replay status.

### Procedure

```bash
# 1. Inspect DLQ messages
kafka-console-consumer \
  --bootstrap-server $KAFKA_BOOTSTRAP_SERVERS \
  --topic stream-dlq \
  --from-beginning \
  --max-messages 10

# 2. Extract rawEvent payloads from DLQ messages
# (DLQ messages are JSON; extract the "rawEvent" field)
kafka-console-consumer \
  --bootstrap-server $KAFKA_BOOTSTRAP_SERVERS \
  --topic stream-dlq \
  --from-beginning | \
  jq -r '.rawEvent' > replay-events.jsonl

# 3. Replay extracted events to the source topic
kafka-console-producer \
  --bootstrap-server $KAFKA_BOOTSTRAP_SERVERS \
  --topic <source-topic> < replay-events.jsonl

# 4. Monitor pipeline for successful processing
# Check DLQ for new failures, check sink for expected records
```

### Post-Replay Verification

- Confirm all replayed records appear in the sink.
- Check DLQ topic for new failures (indicates the fix was incomplete).
- Monitor `dlq.published_count` metric — should not increase.

---

## 6. Planned Design: Airflow Replay DAG

> **NOT YET IMPLEMENTED** — The following describes a planned design for automated DLQ replay. It has not been built.

### 6.1 Overview
```
DLQ Kafka Topic
│
▼
Replay DAG (Airflow)
├── Validate Batch
├── Transform / Deduplicate
├── Replay to Source Topic
└── Monitor & Log Results
```

### 6.2 Features

- Schema and data validation before replay
- Controlled batch replay (by partition or time window)
- Isolation from streaming jobs to prevent backpressure
- Logging of replay metadata (topic, offsets, batch size, timestamp)

### 6.3 Operational Workflow

| Step | Phase | Description |
|------|--------|-------------|
| **1. Diagnose** | Identify error type and DLQ volume using logs or metrics. |
| **2. Validate** | Inspect sample DLQ messages, verify format and schema. |
| **3. Fix & Redeploy** | Patch the job and confirm new version runs cleanly. |
| **4. Run Replay DAG** | Replay validated messages back into the source topic. |
| **5. Monitor** | Track replay throughput, DLQ growth, and sink latency. |
| **6. Verify & Cleanup** | Confirm all records processed; clean DLQ if resolved. |

### 6.4 Replay DAG Operators

| Operator | Purpose | Notes |
|-----------|----------|-------|
| `DlqValidateOperator` | Validates message structure and schema | Fails early on invalid payloads |
| `KafkaReplayOperator` | Re-ingests messages to source topic | Batch replay with offset control |
| `ReplayAuditOperator` | Records replay metrics and metadata | Logs to S3 or internal DB |

---

## Summary

| Capability | Status |
|-----------|--------|
| DLQ publishing (`DLQPublisher`) | **Implemented** — 9 publishing points, 6 error types |
| DLQ event structure (`DlqEvent`) | **Implemented** — 6 fields with static factory |
| Manual replay | **Available** — via Kafka CLI tools |
| Automated Airflow replay DAG | **Planned** — design documented, not yet built |
| DLQ metrics | **Implemented** — `dlq.published_count`, `dlq.failed_count` |
