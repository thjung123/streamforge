# Checkpoint & Savepoint Strategy

This document describes how StreamForge pipelines use checkpoints and savepoints for **state management**, **fault tolerance**, and **recovery**.

> **Status:** Checkpointing and an optional durable `CHECKPOINT_DIR` are implemented in `CheckpointConfig`. Without `CHECKPOINT_DIR`, checkpoints use in-memory JobManager storage. Cluster deployment (an S3 state backend, the Kubernetes Flink Operator) is out of scope for this repo.

---

## 1. Overview

Flink provides two main mechanisms for state persistence and recovery:

| Type       | Purpose                                                  | Trigger   |
| ---------- | -------------------------------------------------------- | --------- |
| Checkpoint | Periodic snapshots for automatic fault-tolerant recovery | Automatic |
| Savepoint  | Manual snapshot for controlled restarts or upgrades      | Manual    |

- **Checkpoints** run continuously during normal operation. On failure, Flink automatically restores from the latest checkpoint.
- **Savepoints** are manually triggered for planned events: upgrades, schema migrations, or controlled restarts.

---

## 2. What Gets Checkpointed

StreamForge pipelines persist the following state in checkpoints and savepoints:

| State Type | Example | Stored In | Checkpointed? |
|-----------|---------|-----------|---------------|
| **Kafka consumer offsets** | Consumer group offsets | Flink source state | **YES** |
| **MongoDB resume tokens** | Change stream cursor position | `MongoSplit` (source split state) | **YES** |
| **Deduplicator** | Seen-key set | `ValueState<Boolean>` + TTL | YES |
| **StatefulMerger** | Last payload hash per key | `ValueState<Long>` + TTL | YES |
| **SessionAnalyzer** | Active sessions | Session window state | YES |
| **Materializer** | Latest value per key | `ValueState` + TTL | YES |
| **DynamicJoiner** | Left/right event buffers + sequence counter | `MapState` x2 + `ValueState` + TTL | YES |
| **StaticJoiner** | Reference data | `BroadcastState` | YES |
| **FlowDisruptionDetector** | Disruption flag + timer timestamp | `ValueState<Boolean>` + `ValueState<Long>` | YES |
| **Kafka sink transactional IDs** | EXACTLY_ONCE transaction state | Flink sink state | YES |

On recovery, all checkpointed state is restored atomically — Kafka sources resume from their last committed offset, operators continue from their last known state, and **MongoDB CDC sources reopen the change stream with `resumeAfter(resumeToken)`** from the checkpointed token.

---

## 3. Configuration

Checkpointing is enabled per job through `CheckpointConfig`:

- `enableAtLeastOnce(env)` — `AT_LEAST_ONCE` mode, used by every job. The exactly-once variant uses `EXACTLY_ONCE` mode for the transactional Kafka sink path.
- Interval `60s`, min-pause `30s`, timeout `120s` (derived as `interval`, `interval / 2`, `interval * 2`).
- Set the `CHECKPOINT_DIR` env var for durable checkpoint storage. Without it, checkpoints use in-memory JobManager storage — fine for local runs, not for production.

---

## 4. Savepoint Contents

A savepoint captures operator state plus source positions:

1. **Kafka sources** resume from the saved consumer offset. No events are skipped or duplicated (assuming idempotent sinks or exactly-once mode).
2. **MongoDB Change Stream sources** — the resume token is saved in the source split state. On restore, the cursor reopens with `resumeAfter(resumeToken)`, so events written between the savepoint and restore are replayed (at-least-once).

---

## 5. Recovery vs. Planned Operations

These are two different paths and are easy to conflate. **Failures recover automatically from checkpoints** — the everyday path, no operator action. **Savepoints are for deliberate lifecycle operations**, not failure recovery.

### Automatic recovery — checkpoints

A task/node failure, OOM, or transient error triggers an automatic restart from the latest checkpoint. Flink restores all operator state, Kafka offsets, and the MongoDB resume token; the CDC source resumes via `resumeAfter`. Duplicates after replay are absorbed by the `Deduplicator` and idempotent sinks.

Bad *events* (not a failure) are routed to the DLQ instead — fix the cause and replay from the DLQ, see the [DLQ Replay Guide](dlq-replay-guide.md).

### Planned operations — savepoints

| Operation | Procedure |
|-----------|-----------|
| **Redeploy / code upgrade** | Savepoint → stop → deploy new JAR → restore. A retained checkpoint (last-state) works too and is faster for rolling deploys. |
| **Flink version upgrade** | Canonical savepoint → upgrade → restore. |
| **Rescaling (parallelism change)** | Savepoint redistributes keyed state across the new parallelism. |
| **Schema / state migration** | Savepoint → deploy with the new state schema → restore (needs compatible state evolution). |

---

## 6. Processing Guarantees

| Guarantee | Requirement |
|-----------|-------------|
| At-least-once | Default with checkpointing enabled. Applies to Kafka sources. |
| Exactly-once (Kafka sink) | Use `KafkaSinkBuilder.exactlyOnce(transactionalIdPrefix)`. Configures `enable.idempotence=true` and `transaction.timeout.ms=900000`. |
| Idempotent writes (MongoDB sink) | `MongoSinkBuilder` upserts by `_id` — natural deduplication. |
| Idempotent writes (ES sink) | `ElasticsearchSinkBuilder` indexes by `traceId` — natural deduplication. |
| MongoDB CDC source | **At-least-once** — the resume token is checkpointed; on recovery the stream resumes via `resumeAfter`, replaying events written during downtime. |

With at-least-once, duplicate events may occur after checkpoint recovery. Use the **Deduplicator** pattern to handle this at the stream level.

---

## 7. Operational Recommendations

- Monitor checkpoint duration and alignment time to detect state size or backpressure issues.
- Periodically clean up old savepoints to manage storage costs.
- Automate savepoint creation in CI/CD before deploying new versions.
- Test savepoint restore in staging before production upgrades.

---

## 8. Known Gaps

1. **Oplog rollover.** The resume token is checkpointed, but if a MongoDB CDC job is down long enough for the oplog to roll past that token, the change stream can't be reopened and needs a fresh start (from the current position, with possible loss).

2. **No state migration tooling.** There is no built-in tooling for migrating operator state schemas across versions. State schema changes require compatible Flink state evolution or a fresh start.
