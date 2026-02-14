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

| State Type | Example | Stored In |
|-----------|---------|-----------|
| **Source offsets** | Kafka consumer offsets, MongoDB resume tokens | Flink source state |
| **Operator state** | Deduplicator seen-keys, StatefulMerger previous values | `ValueState` / `MapState` |
| **Sink state** | Kafka transactional IDs (exactly-once) | Flink sink state |
| **Pattern state** | SessionAnalyzer active sessions, Materializer latest values | `ValueState` / `ListState` |

On recovery, all state is restored atomically — sources resume from their last committed offset/token, and operators continue from their last known state.

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

When a savepoint is taken, source offsets (Kafka offsets, MongoDB resume tokens) are included. On restore:

1. Kafka sources resume from the saved consumer offset.
2. MongoDB Change Stream sources reopen the cursor from the saved resume token.
3. No events are skipped or duplicated (assuming idempotent sinks or exactly-once mode).

---

## 5. Recovery Scenarios

| Scenario | Recovery Method | Details |
|----------|----------------|---------|
| **Node failure** | Latest **checkpoint** (automatic) | Flink restores all operator state and source offsets. |
| **Redeploy / Upgrade** | **Savepoint** (manual) | Trigger savepoint → deploy new version → restore from savepoint. |
| **Data corruption** | **Savepoint** or **checkpoint** | Restore from a known-good snapshot. Replay from DLQ if needed. |
| **Schema change** | **Savepoint** (manual) | Stop job → create savepoint → deploy with new schema → restore. |
| **Parallelism change** | **Savepoint** (manual) | Savepoint redistributes keyed state across new parallelism. |

---

## 6. Processing Guarantees

| Guarantee | Requirement |
|-----------|-------------|
| At-least-once | Default with checkpointing enabled. |
| Exactly-once | Sinks must support idempotent writes or transactional commits (see TransactionalWriter pattern). |

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
