# Fault Tolerance

This document describes how StreamForge handles failures at each layer of the pipeline — from source to sink — and the recovery mechanisms available.

---

## 1. Failure Layers

| Layer | Failure Type | Impact | Recovery |
|-------|-------------|--------|----------|
| **Source** | Connection lost, cursor expired | No new events ingested | Auto-reconnect; restore from checkpoint (resume token / offset) |
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
- Resume token is stored in Flink checkpoint state.
- On recovery, the Change Stream cursor reopens from the saved resume token.
- If the resume token has expired (oplog rolled over), a full re-sync is required — see **SnapshotLoader** pattern.

---

## 3. Operator Recovery

Flink operators recover their state from checkpoints automatically. StreamForge patterns that maintain state:

| Pattern | State Type | Recovery Behavior |
|---------|-----------|-------------------|
| **Deduplicator** | `ValueState<Boolean>` + TTL | Seen-key set restored; TTL continues from checkpoint time |
| **StatefulMerger** | `ValueState` (previous record) | Previous state restored; diff comparison continues |
| **SessionAnalyzer** | Session window state | Active sessions restored; late events handled by `allowedLateness` |
| **Materializer** | `ValueState` (latest per key) | Materialized table state restored |

If an operator fails due to a code bug, fix the issue, create a savepoint, and redeploy. The savepoint preserves all operator state across the code change (as long as state schema is compatible).

---

## 4. Sink Recovery

### Retry Strategy
- Transient failures (network timeout, temporary unavailability) are retried with exponential backoff.
- The retry count and interval are configurable per sink.

### DLQ Routing
- Events that fail after all retries are routed to the DLQ topic.
- Each DLQ event includes: error code, error message, original payload, source stage, and stack trace.
- See [DLQ Replay Guide](dlq-replay-guide.md) for the replay procedure.

### Exactly-Once Sinks
- Kafka sinks can use EXACTLY_ONCE mode with transactional writes (see TransactionalWriter pattern).
- MongoDB sinks use idempotent `replaceOne` with `_id` key — natural deduplication on retry.

---

## 5. Recovery Decision Tree

```
Failure detected
│
├── Automatic? (node crash, transient error)
│   └── Flink restores from latest checkpoint → done
│
├── Planned? (upgrade, schema change)
│   └── Trigger savepoint → deploy new version → restore from savepoint
│
├── Data issue? (bad events causing errors)
│   └── Events routed to DLQ → fix pipeline → replay from DLQ
│
└── State corruption? (incompatible state schema)
    └── Restore from older savepoint or start fresh with SnapshotLoader
```

---

## 6. Monitoring for Fault Detection

| Metric | What It Detects |
|--------|----------------|
| `checkpoint_duration_ms` | State size growth or backpressure issues |
| `checkpoint_failure_count` | Checkpoint storage or alignment problems |
| `source_latency_ms` | Source falling behind or stalled |
| `dlq_event_count` | Persistent processing failures |
| `restarts` (Flink) | Frequent automatic recovery cycles |

Set alerts on these metrics to detect issues before they escalate.
