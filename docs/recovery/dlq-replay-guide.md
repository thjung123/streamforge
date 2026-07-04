# DLQ Replay Guide

This document describes the **DLQ (Dead Letter Queue)** implementation in StreamForge and the replay procedure for failed events.

---

> **Status**
> - **DLQ publishing**: IMPLEMENTED — `DLQPublisher` routes failed events to a Kafka DLQ topic across 9 publishing points.
> - **Replay**: manual (see §5). Automated replay is not built.

---

## 1. Purpose & Scope

The **Dead Letter Queue (DLQ)** stores events that failed during parsing, transformation, quality enforcement, or sink operations. It ensures data durability and allows reprocessing once the underlying issue has been fixed.

This guide covers:
- Current DLQ implementation (what is built today)
- DLQ event structure and error types
- All publishing points
- Manual replay procedure

---

## 2. Current Implementation

### DLQPublisher

`DLQPublisher` (`com.streamforge.core.dlq.DLQPublisher`) is a thread-safe singleton that publishes failed events to a Kafka DLQ topic.

- **Transport**: Standalone `KafkaProducer<String, String>` (not Flink's transactional sink).
- **Topic**: `DLQ_TOPIC` (required; the value `stream-dlq` comes from `streamforge.json`, and the job fails fast if it's unset).
- **Serialization**: `DlqEvent` → JSON via `JsonUtils.toJson()`.
- **Delivery**: Async, non-blocking send with callback (never stalls the task thread). Durable on ack via `acks=all` + `enable.idempotence=true` + `retries=MAX`, with a shutdown-hook flush. At-least-once: on recovery a bad event can be re-published, so duplicate dead-letters are expected.
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
| 9 | `SchemaEnforcer` | `SCHEMA_VIOLATION` | Payload fails validation against all configured schema versions |

---

## 3. Failure Categories

| Error Type | Source | Description | Resolution |
|-----------|--------|-------------|------------|
| `SOURCE_PARSING_ERROR` | `MongoChangeStreamSource` | Failed to read a change stream event | Fix MongoDB configuration or schema, then replay |
| `PARSING_ERROR` | `StreamEnvelopParser`, `MongoToKafkaParser` | Failed to deserialize or parse input | Fix schema or input format, redeploy, then replay |
| `PROCESSING_ERROR` | `MongoToKafkaProcessor`, `KafkaToMongoProcessor` | Transformation or enrichment logic failed | Patch and redeploy pipeline, then replay |
| `SCHEMA_VIOLATION` | `SchemaEnforcer` | Payload fails validation against all configured schema versions | Update schema rules or fix upstream producer |
| `CONSTRAINT_VIOLATION` | `ConstraintEnforcer` | Business rule validation failed | Update constraint rules or fix upstream data |
| `SINK_ERROR` | `MongoSinkBuilder`, `KafkaSinkBuilder` | Failed to write to target | Fix connection/configuration, then replay |

---

## 4. DLQ Policy

- Each pipeline writes to a shared **Kafka DLQ topic** (`DLQ_TOPIC`, required; `stream-dlq` in `streamforge.json`).
- Events in DLQ are **not reprocessed automatically**.
- Replay should only occur **after** the cause of failure has been identified and fixed.
- Retention is typically **7–14 days** to allow safe recovery without backlog growth.

---

## 5. Manual Replay (Current Workaround)

Replay is manual. Once the root cause is fixed, use this procedure:

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

## Summary

| Capability | Status |
|-----------|--------|
| DLQ publishing (`DLQPublisher`) | **Implemented** — 9 publishing points, 6 error types |
| DLQ event structure (`DlqEvent`) | **Implemented** — 6 fields with static factory |
| Manual replay | **Available** — via Kafka CLI tools |
| Automated replay | **Not built** — manual procedure only |
| DLQ metrics | **Implemented** — `dlq.published_count`, `dlq.failed_count` |
