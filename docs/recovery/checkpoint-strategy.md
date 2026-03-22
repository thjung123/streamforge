# Checkpoint & Savepoint Strategy

This document defines how StreamForge pipelines use checkpoints and savepoints for **state management**, **fault tolerance**, and **recovery** in production.

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
| **MongoDB resume tokens** | Change stream cursor position | — | **NOT STORED** (`snapshotState()` returns empty list) |
| **Deduplicator** | Seen-key set | `ValueState<Boolean>` + TTL | YES |
| **StatefulMerger** | Previous record per key | `ValueState` | YES |
| **SessionAnalyzer** | Active sessions | Session window state | YES |
| **Materializer** | Latest value per key | `ValueState` | YES |
| **DynamicJoiner** | Left/right event buffers + sequence counter | `MapState` x2 + `ValueState` + TTL | YES |
| **StaticJoiner** | Reference data | `BroadcastState` | YES |
| **FlowDisruptionDetector** | Disruption flag + timer timestamp | `ValueState<Boolean>` + `ValueState<Long>` | YES |
| **Kafka sink transactional IDs** | EXACTLY_ONCE transaction state | Flink sink state | YES |

On recovery, all checkpointed state is restored atomically — Kafka sources resume from their last committed offset, and operators continue from their last known state. **MongoDB CDC sources do NOT restore from checkpoint** — they reconnect from the current server position.

---

## 3. Checkpoint Configuration (via Flink Operator YAML)

Instead of configuring checkpoints in Java, define them in the **FlinkDeployment YAML** for consistency across environments.

```yaml
spec:
  job:
    upgradeMode: "savepoint"
  flinkConfiguration:
    state.savepoints.dir: s3://streamforge-state/savepoints
    state.checkpoints.dir: s3://streamforge-state/checkpoints
    execution.checkpointing.interval: 60s
    execution.checkpointing.timeout: 120s
    execution.checkpointing.min-pause: 30s
    execution.checkpointing.externalized-checkpoint-retention: RETAIN_ON_CANCELLATION
```

**Guidelines**

- Interval: 30–60s — frequent enough to minimize data reprocessing on failure.
- Timeout: 60–120s — longer if sink latency is high.
- Min Pause: ~30s — prevents checkpoint backpressure.
- Externalized Checkpoints: Always enabled (RETAIN_ON_CANCELLATION) for safe rollback.
- Storage: Use S3 or HDFS for persistent state across deployments.

---

## 4. Savepoint Usage

Use savepoints for controlled restart scenarios:

- **Version upgrades** — before deploying a new JAR.
- **Schema migrations** — state must align with new schema.
- **Pipeline reconfiguration** — changing parallelism, adding/removing patterns.
- **Disaster recovery drills** — controlled rollback points.

### Savepoint with Source Offsets

When a savepoint is taken, source offsets are included **where supported**:

1. **Kafka sources** resume from the saved consumer offset. No events are skipped or duplicated (assuming idempotent sinks or exactly-once mode).
2. **MongoDB Change Stream sources** — resume tokens are **NOT** saved in savepoints (`snapshotState()` returns empty list). On restore, the cursor reopens from the **current server position**. Events between the savepoint and restore **may be missed**.

---

## 5. Recovery Scenarios

| Scenario | Recovery Method | Details |
|----------|----------------|---------|
| **Node failure** | Latest **checkpoint** (automatic) | Flink restores all operator state and Kafka offsets. MongoDB CDC resumes from current position. |
| **Redeploy / Upgrade** | **Savepoint** (manual) | Trigger savepoint → deploy new version → restore from savepoint. MongoDB CDC events during downtime may be missed. |
| **Data corruption** | **Savepoint** or **checkpoint** | Restore from a known-good snapshot. Replay from DLQ if needed. |
| **Schema change** | **Savepoint** (manual) | Stop job → create savepoint → deploy with new schema → restore. |
| **Parallelism change** | **Savepoint** (manual) | Savepoint redistributes keyed state across new parallelism. |

---

## 6. Processing Guarantees

| Guarantee | Requirement |
|-----------|-------------|
| At-least-once | Default with checkpointing enabled. Applies to Kafka sources. |
| Exactly-once (Kafka sink) | Use `KafkaSinkBuilder.exactlyOnce(transactionalIdPrefix)`. Configures `enable.idempotence=true` and `transaction.timeout.ms=900000`. |
| Idempotent writes (MongoDB sink) | `MongoSinkBuilder` uses `replaceOne` by `_id` — natural deduplication. |
| Idempotent writes (ES sink) | `ElasticsearchSinkBuilder` indexes by `traceId` — natural deduplication. |
| MongoDB CDC source | **At-most-once** — resume tokens are not checkpointed; events may be missed on recovery. |

With at-least-once, duplicate events may occur after checkpoint recovery. Use the **Deduplicator** pattern to handle this at the stream level.

---

## 7. Operator-Driven Savepoint Management

In Kubernetes-based deployments, the **Flink Operator** automates savepoint lifecycle during job events.

### 7.1 Automatic Savepoint on Job Cancel

```yaml
spec:
  flinkConfiguration:
    kubernetes.operator.job.cancel.mode: savepoint
    kubernetes.operator.job.savepoint.cleanup: retain
```

**Behavior**:

- **On Cancel or Upgrade**: A savepoint is triggered automatically and stored under `state.savepoints.dir`.
- **On Restart**: The Operator restores from the latest valid savepoint.
- **Retention**: Savepoints are retained unless cleaned by lifecycle rules or manual action.

### 7.2 Operational Workflow

| Step | Action | Description |
|------|---------|-------------|
| **1. Deploy** | Apply `FlinkDeployment` | Operator starts the job with checkpointing and savepoint configuration. |
| **2. Upgrade** | Modify job spec or image | Operator takes savepoint → cancels job → restores new job from savepoint. |
| **3. Failure Recovery** | Automatic | Operator recovers from latest checkpoint; if unavailable, falls back to most recent savepoint. |
| **4. Retention** | Periodic cleanup | S3 lifecycle rules expire old savepoints; critical ones can be tagged and retained. |

---

## 8. Operational Recommendations

- Monitor checkpoint duration and alignment time to detect state size or backpressure issues.
- Periodically clean up old savepoints to manage storage costs.
- Automate savepoint creation in CI/CD before deploying new versions.
- Test savepoint restore in staging before production upgrades.

---

## 9. Known Gaps

1. **MongoDB resume token gap.** `MongoChangeStreamSource.snapshotState()` returns `Collections.emptyList()`. Resume tokens are not included in checkpoints or savepoints. On any recovery (automatic or manual), the MongoDB CDC source reconnects from the current server position. Events occurring between failure and reconnection are lost. This is a fundamental limitation of the current `MongoChangeStreamSource` implementation.

2. **No state migration tooling.** There is no built-in tooling for migrating operator state schemas across versions. State schema changes require compatible Flink state evolution or a fresh start with potential data loss from the MongoDB CDC source.
