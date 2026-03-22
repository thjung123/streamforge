# Fault Tolerance

This document describes how StreamForge handles failures at each layer of the pipeline — from source to sink — and the recovery mechanisms available.

---

## 1. Failure Layers

| Layer | Failure Type | Impact | Recovery |
|-------|-------------|--------|----------|
| **Source (Kafka)** | Connection lost, offset expired | No new events ingested | Auto-reconnect; restore from checkpoint (consumer offset) |
| **Source (MongoDB)** | Connection lost, cursor expired | No new events ingested | Auto-reconnect from **current position**; resume token is **NOT** checkpointed |
| **Operator** | OOM, serialization error, logic bug | Events lost or stuck | Flink restarts from checkpoint; fix and redeploy for logic bugs |
| **Sink** | Target unreachable, write rejected | Events not delivered | Retry with backoff; DLQ for persistent failures |
| **Infrastructure** | Node crash, network partition | Job killed | Flink restores from latest checkpoint on healthy node |

---

## 2. Source Recovery

### Kafka Source
- Flink manages consumer offsets in checkpoint state.
- On recovery, the consumer resumes from the last committed offset.
- Duplicate events are possible (at-least-once) — use **Deduplicator** pattern if needed.

### MongoDB Change Stream Source

> **Important**: MongoDB resume tokens are **NOT** checkpointed. `MongoChangeStreamSource.snapshotState()` returns `Collections.emptyList()`.

Actual recovery behavior:
1. On failure, the source **auto-reconnects** to the MongoDB change stream.
2. The cursor reopens from the **current server position** — NOT from a saved resume token.
3. Events that occurred between the failure and reconnection **may be missed**.
4. If the oplog has rolled over during downtime, the change stream cannot be reopened at all — a manual recovery is required.

This means MongoDB CDC provides **at-most-once** delivery semantics for the source layer. Downstream deduplication (via `Deduplicator`) and idempotent sinks mitigate but do not eliminate this gap.

---

## 3. Operator Recovery

Flink operators recover their state from checkpoints automatically. StreamForge patterns that maintain state:

| Pattern | State Type | Recovery Behavior |
|---------|-----------|-------------------|
| **Deduplicator** | `ValueState<Boolean>` + TTL | Seen-key set restored; TTL continues from checkpoint time |
| **StatefulMerger** | `ValueState` (previous record) | Previous state restored; diff comparison continues |
| **SessionAnalyzer** | Session window state | Active sessions restored; late events handled by `allowedLateness` |
| **Materializer** | `ValueState` (latest per key) | Materialized table state restored |
| **DynamicJoiner** | `MapState<Long, BufferedEvent<T>>` x2 + `ValueState<Long>` + TTL | Left/right buffers and sequence counter restored; TTL-expired entries cleaned after restore |
| **StaticJoiner** | `BroadcastState<String, R>` | Reference data broadcast state restored; no TTL — lives until replaced |
| **FlowDisruptionDetector** | `ValueState<Boolean>` + `ValueState<Long>` | Disruption flag and timer timestamp restored; timers re-registered |

If an operator fails due to a code bug, fix the issue, create a savepoint, and redeploy. The savepoint preserves all operator state across the code change (as long as state schema is compatible).

---

## 4. Sink Recovery

### Retry Strategy
- Transient failures (network timeout, temporary unavailability) are retried with exponential backoff.
- The retry count and interval are configurable per sink.

### DLQ Routing

Events that fail after all retries are routed to the DLQ topic via `DLQPublisher`. Each DLQ event includes: error type, error message, source operator, timestamp, original payload, and stack trace.

**Error types by publishing point:**

| # | Publishing Point | Error Type | Trigger |
|---|-----------------|------------|---------|
| 1 | `StreamEnvelopParser` | `PARSING_ERROR` | JSON deserialization failure |
| 2 | `MongoToKafkaParser` | `PARSING_ERROR` | CDC document parse failure |
| 3 | `MongoToKafkaProcessor` | `PROCESSING_ERROR` | Transformation logic error |
| 4 | `KafkaToMongoProcessor` | `PROCESSING_ERROR` | Transformation logic error |
| 5 | `MongoChangeStreamSource` | `SOURCE_PARSING_ERROR` | Exception reading change stream event |
| 6 | `MongoSinkBuilder` | `SINK_ERROR` | Exception writing to MongoDB |
| 7 | `KafkaSinkBuilder` | `SINK_ERROR` | Exception in pre-sink metrics map function |
| 8 | `ConstraintEnforcer` | `CONSTRAINT_VIOLATION` | Business rule validation failure |
| 9 | `SchemaEnforcer` | `SCHEMA_VIOLATION` | Schema version mismatch |

See [DLQ Replay Guide](dlq-replay-guide.md) for the replay procedure.

### Exactly-Once Sinks

- **Kafka**: Use `KafkaSinkBuilder.exactlyOnce(transactionalIdPrefix)` for EXACTLY_ONCE delivery. Also available: `.compacted()` (AT_LEAST_ONCE + log compaction) and `.compactedExactlyOnce(prefix)` (EXACTLY_ONCE + log compaction). When EXACTLY_ONCE is enabled, the builder automatically configures `enable.idempotence=true` and `transaction.timeout.ms=900000`.
- **MongoDB**: Uses idempotent `replaceOne` with `_id` key — natural deduplication on retry.
- **Elasticsearch**: Uses idempotent index by `traceId` — natural deduplication on retry.

---

## 5. Recovery Decision Tree

```
Failure detected
│
├── Automatic? (node crash, transient error)
│   └── Flink restores from latest checkpoint → done
│       Note: Kafka offsets restored; MongoDB resume tokens NOT restored
│
├── Planned? (upgrade, schema change)
│   └── Trigger savepoint → deploy new version → restore from savepoint
│       Note: MongoDB CDC events during stop/restart window may be missed
│
├── Data issue? (bad events causing errors)
│   └── Events routed to DLQ → fix pipeline → replay from DLQ
│
└── State corruption? (incompatible state schema)
    └── Restore from older savepoint, or stop job and restart fresh
        (MongoDB CDC will resume from current server position, not from
        the point of corruption — some events may be missed)
```

---

## 6. Monitoring for Fault Detection

| Metric | What It Detects |
|--------|----------------|
| `checkpoint_duration_ms` | State size growth or backpressure issues |
| `checkpoint_failure_count` | Checkpoint storage or alignment problems |
| `source_latency_ms` | Source falling behind or stalled |
| `dlq.published_count` | Persistent processing failures |
| `dlq.failed_count` | DLQ publishing failures (DLQ itself is down) |
| `restarts` (Flink) | Frequent automatic recovery cycles |

Set alerts on these metrics to detect issues before they escalate.

---

## 7. Known Limitations

1. **MongoDB resume tokens are not checkpointed.** `MongoChangeStreamSource.snapshotState()` returns `Collections.emptyList()`. On recovery, the source reconnects from the current server position, not from a saved resume token. Events between failure and reconnection may be missed.

2. **DLQ publishing is non-transactional.** `DLQPublisher` uses a standalone `KafkaProducer` (not Flink's transactional sink). If the job fails between processing an event and publishing the DLQ record, the DLQ event may be lost. This is a best-effort mechanism.

3. **No automated DLQ replay.** DLQ events must be replayed manually or via custom tooling. The Airflow-based replay DAG described in the [DLQ Replay Guide](dlq-replay-guide.md) is a planned design, not yet implemented.
