# Checkpoint & Savepoint Strategy

This document defines how our **Flink CDC pipeline** uses checkpoints and savepoints for **state management**, **fault tolerance**, and **recovery** in production.

---

## 1. Overview

Flink provides two main mechanisms for state persistence and recovery:

| Type       | Purpose                                                  | Trigger   |
| ---------- | -------------------------------------------------------- | --------- |
| Checkpoint | Periodic snapshots for automatic fault-tolerant recovery | Automatic |
| Savepoint  | Manual snapshot for controlled restarts or upgrades      | Manual    |

- **Checkpoints** are continuous and used for automatic recovery after failures.
- **Savepoints** are manually triggered, typically for upgrades, schema migrations, or controlled restarts.

---

## 2. Checkpoint Strategy (via Flink Operator YAML)

Instead of configuring checkpoints in Java, we define them directly in the **FlinkDeployment YAML** to ensure consistency across environments.

Example (`FlinkDeployment`):

```yaml
spec:
  job:
    upgradeMode: "savepoint"
  flinkConfiguration:
    state.savepoints.dir: s3://flink-checkpoints/savepoints
    state.checkpoints.dir: s3://flink-checkpoints/checkpoints
    execution.checkpointing.interval: 60s
    execution.checkpointing.timeout: 120s
    execution.checkpointing.min-pause: 30s
    execution.checkpointing.externalized-checkpoint-retention: RETAIN_ON_CANCELLATION
```

**Practical Guidelines**

- Interval: 30–60s — frequent enough to minimize data reprocessing.
- Timeout: 60–120s — longer if sink latency is high.
- Min Pause: ~30s — prevents checkpoint backpressure.
- Externalized Checkpoints: Always enabled (RETAIN_ON_CANCELLATION) for safe rollback.
- Storage: Use S3 or HDFS for persistent state across deployments.

---

## 3. Savepoint Usage

Use savepoints when you need controlled restart scenarios such as:

- Version upgrades — before deploying a new JAR.
- Schema migrations — state must align with new schema.
- Disaster recovery drills — controlled rollback points.

---

## 4. Recovery Scenarios

| Scenario               | Recommended Action                                           |
| ---------------------- | ------------------------------------------------------------ |
| **Node failure**       | Job automatically recovers from the latest **checkpoint**.   |
| **Redeploy / Upgrade** | Trigger a **savepoint** before deployment and restore state from it. |
| **Data corruption**    | Restore from a **savepoint** or **checkpoint**, then **replay from DLQ** if needed. |
| **Schema change**      | Stop the job → Create **savepoint** → Deploy new version → Restore from savepoint. |

---

## 5. Processing Guarantees

With this setup:

- At-least-once delivery is guaranteed by default.
- To achieve exactly-once, sinks must support idempotent writes or transactional commits.

---

## 6. Operational Recommendations

- Monitor checkpoint duration and alignment time in Grafana to detect bottlenecks.
- Periodically clean up old savepoints to manage storage costs.
- Automate savepoint creation in CI/CD before deploying new versions.

---

## 7. Operator-Driven Savepoint Management

In Kubernetes-based deployments, the **Flink Operator** ensures safe and consistent state transitions during job lifecycle events such as upgrades, restarts, or controlled shutdowns.

This section defines how savepoints are managed and recovered in production environments.

---

### 7.1 Automatic Savepoint on Job Cancel

When `kubernetes.operator.job.cancel.mode: savepoint` is enabled, the Operator ensures that every controlled stop (e.g., redeploy, upgrade, rollback) produces a consistent **savepoint** before terminating the job.

```yaml
spec:
  flinkConfiguration:
    kubernetes.operator.job.cancel.mode: savepoint
    kubernetes.operator.job.savepoint.cleanup: retain
```

**Behavior**:

- **On Cancel or Upgrade**: A savepoint is triggered automatically and stored under state.savepoints.dir (e.g., s3://flink-checkpoints/savepoints).
- **On Restart**: The Operator restores from the latest valid savepoint.
- **Retention**: Savepoints are retained unless cleaned by lifecycle rules or manual action.

### 7.2 Operational Workflow

| Step | Action | Description |
|------|---------|-------------|
| **1. Deploy** | Apply or sync `FlinkDeployment` | Operator starts the job with checkpointing and savepoint configuration. |
| **2. Upgrade** | Modify job spec or image | Operator automatically takes a savepoint, cancels the current job, and restores the new job from that savepoint. |
| **3. Failure Recovery** | Handled automatically | Operator recovers from the latest checkpoint; if unavailable, it restores from the most recent savepoint. |
| **4. Retention Policy** | Periodic cleanup | S3 lifecycle rules automatically expire old savepoints; critical ones can be tagged and retained longer. |
````