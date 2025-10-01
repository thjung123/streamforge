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
- For external systems like Redis or JDBC, additional idempotency logic may be required.

---

## 6. Operational Recommendations

- Monitor checkpoint duration and alignment time in Grafana to detect bottlenecks.
- Periodically clean up old savepoints to manage storage costs.
- Automate savepoint creation in CI/CD before deploying new versions.

---

## 7. Savepoint Automation (CLI Tools)

Use helper scripts to automate savepoint lifecycle in CI/CD or manual operations:

```bash
# Trigger savepoint for a running job
./scripts/trigger_savepoint.sh <job_id>

# Restore job from a specific savepoint
./scripts/restore_from_savepoint.sh <savepoint_path>
```