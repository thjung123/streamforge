# Fault Tolerance

This document describes how StreamForge handles failures at each layer of the pipeline — from source to sink — and the recovery mechanisms available.

---

## 1. Failure Layers

| Layer | Failure Type | Impact | Recovery |
|-------|-------------|--------|----------|
| **Source (Kafka)** | Connection lost, offset expired | No new events ingested | Auto-reconnect; restore from checkpoint (consumer offset) |
| **Source (MongoDB)** | Connection lost, cursor expired | No new events ingested | Reopen with `resumeAfter(resumeToken)` from the checkpointed token; reconnect resumes from it too |
| **Operator** | OOM, serialization error, logic bug | Events lost or stuck | Flink restarts from checkpoint; fix and redeploy for logic bugs |
| **Sink** | Target unreachable, write rejected | Events not delivered | Kafka producer retries; Mongo sink routes failures to DLQ |
| **Infrastructure** | Node crash, network partition | Job killed | Flink restores from latest checkpoint on healthy node |

---

## 2. Source Recovery

### Kafka Source
- Flink manages consumer offsets in checkpoint state.
- On recovery, the consumer resumes from the last committed offset.
- Duplicate events are possible (at-least-once) — use **Deduplicator** pattern if needed.

### MongoDB Change Stream Source

The reader records each change's resume token in its split (`MongoSplit`) and returns it from `snapshotState()`, so it travels in the checkpoint.

Recovery behavior:
1. On restore, the split's resume token is restored and the cursor reopens with `watch().resumeAfter(resumeToken)`.
2. Events written between failure and restore are replayed from that token.
3. A transient reconnect uses the same token, so it resumes without a gap.
4. If the oplog has rolled past the token during a long downtime, it's no longer valid and the stream can't be reopened — a fresh start (from the current position, with possible loss) is required.

This gives the source **at-least-once** semantics. With the `Deduplicator` and idempotent sinks, the pipeline is effectively-once.

---

## 3. Operator Recovery

Flink operators recover their state from checkpoints automatically. StreamForge patterns that maintain state:

| Pattern | State Type | Recovery Behavior |
|---------|-----------|-------------------|
| **Deduplicator** | `ValueState<Boolean>` + TTL | Seen-key set restored; TTL continues from checkpoint time |
| **StatefulMerger** | `ValueState<Long>` (payload hash) + TTL | Last-seen payload hash restored; no-op suppression continues |
| **SessionAnalyzer** | Session window state | Active sessions restored; late events handled by `allowedLateness` |
| **Materializer** | `ValueState` (latest per key) + TTL | Materialized table state restored |
| **DynamicJoiner** | `MapState<Long, BufferedEvent<T>>` x2 + `ValueState<Long>` + TTL | Left/right buffers and sequence counter restored; TTL-expired entries cleaned after restore |
| **StaticJoiner** | `BroadcastState<String, R>` | Reference data broadcast state restored; no TTL — lives until replaced |
| **FlowDisruptionDetector** | `ValueState<Boolean>` + `ValueState<Long>` | Disruption flag and timer timestamp restored; timers re-registered |

If an operator fails due to a code bug, fix the issue, create a savepoint, and redeploy. The savepoint preserves all operator state across the code change (as long as state schema is compatible).

---

## 4. Sink Recovery

### Retry Strategy
- The Kafka sink producer retries transient failures (`retries=10`).
- The Mongo sink does not retry: a failed `bulkWrite` routes the affected records to the DLQ by their index.

### DLQ Routing

Events that fail are routed to the DLQ topic via `DLQPublisher`. Each DLQ event includes: error type, error message, source, timestamp, raw event, and stack trace.

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
| 9 | `SchemaEnforcer` | `SCHEMA_VIOLATION` | Payload fails schema validation |

See [DLQ Replay Guide](dlq-replay-guide.md) for the replay procedure.

### Exactly-Once Sinks

- **Kafka**: Use `KafkaSinkBuilder.exactlyOnce(transactionalIdPrefix)` for EXACTLY_ONCE delivery. Also available: `.compacted()` (AT_LEAST_ONCE + log compaction). When EXACTLY_ONCE is enabled, the builder automatically configures `enable.idempotence=true` and `transaction.timeout.ms=900000`.
- **MongoDB**: Uses idempotent `replaceOne` with `_id` key — natural deduplication on retry.
- **Elasticsearch**: Uses idempotent index by `traceId` — natural deduplication on retry.

---

## 5. Recovery Decision Tree

```
Failure detected
│
├── Automatic? (node crash, transient error)
│   └── Flink restores from latest checkpoint → done
│       Note: Kafka offsets and the MongoDB resume token are both restored
│
├── Planned? (upgrade, schema change)
│   └── Trigger savepoint → deploy new version → restore from savepoint
│       Note: MongoDB CDC replays from the checkpointed resume token
│
├── Data issue? (bad events causing errors)
│   └── Events routed to DLQ → fix pipeline → replay from DLQ
│
└── State corruption? (incompatible state schema)
    └── Restore from older savepoint, or stop job and restart fresh
        (a fresh start drops the resume token, so the CDC source
        restarts from now — earlier events are not replayed)
```

---

## 6. Monitoring for Fault Detection

Metrics are exposed for Prometheus by enabling the reporter (set `METRICS_PORT`, e.g. `9249`); scrape
that endpoint and build dashboards/alerts in Grafana. The app emits `dlq.*` and the pattern-level
counters in `MetricKeys`; Flink emits its own checkpoint, restart, and backpressure metrics.

---

## 7. Known Limitations

1. **DLQ publishing is outside Flink checkpoints.** `DLQPublisher` uses a standalone `KafkaProducer` (not Flink's transactional sink) with an async durable send (`acks=all` + idempotence). It's at-least-once: on recovery the source replays, so a bad event can be re-published to the DLQ. Duplicate dead-letters are expected and harmless.

2. **Oplog rollover.** If a MongoDB CDC job is down long enough for the oplog to roll past the checkpointed resume token, the stream can't resume and needs a fresh start, with possible loss.

3. **No automated DLQ replay.** DLQ events are replayed manually; see the [DLQ Replay Guide](dlq-replay-guide.md) for the procedure.
