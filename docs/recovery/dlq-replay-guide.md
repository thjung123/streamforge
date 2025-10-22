# DLQ Replay Guide

This document defines the **DLQ (Dead Letter Queue) replay procedure** used in the Flink CDC pipeline.  
It explains how failed events stored in DLQ topics are safely validated and reprocessed using a controlled replay workflow.

---

## 1. Purpose & Scope

The **Dead Letter Queue (DLQ)** temporarily stores events that failed during parsing, transformation, or sink operations.  
It ensures data durability and allows manual reprocessing once the underlying issue has been fixed.

This guide covers:
- DLQ operational policy
- Replay prerequisites
- Airflow-based replay workflow
- Post-replay verification and monitoring

---

## 2. DLQ Design & Policy

- Each pipeline maintains its own **Kafka DLQ topic** (e.g., `cdc.<pipeline>.dlq`).
- Events in DLQ are **not reprocessed automatically**.
- Replay should only occur **after** the cause of failure has been identified and fixed.
- Retention is typically **7–14 days** to allow safe recovery without backlog growth.

---

## 3. Failure Categories

| Stage | Error Type | Description | Resolution |
|--------|-------------|--------------|-------------|
| **Source** | `PARSING_ERROR` | Failed to deserialize or parse input | Fix schema or input format, redeploy, then replay |
| **Processor** | `PROCESSING_ERROR` | Transformation or enrichment logic failed | Patch and redeploy pipeline, then replay |
| **Sink** | `SINK_ERROR` | Failed to write to Redis/JDBC/S3 | Fix configuration or connection, then replay |

---

## 4. Replay Preconditions

Before running a replay, ensure that:

1. The root cause has been identified and fixed (schema, code, sink config, etc.).
2. DLQ messages have been inspected and validated.
3. Downstream systems are healthy and can accept data.
4. Monitoring and alerting are active to observe replay status.
5. Replay range and batch size are clearly defined.

---

## 5. Replay Architecture

DLQ replay in production is handled through a **dedicated Airflow DAG** or **on-demand batch job**.  
This approach ensures safe reprocessing, observability, and operational control.

### 5.1 Overview
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


### 5.2 Features

- Schema and data validation before replay
- Controlled batch replay (by partition or time window)
- Isolation from streaming jobs to prevent backpressure
- Logging of replay metadata (topic, offsets, batch size, timestamp)

---

## 6. Operational Workflow

| Step | Phase | Description |
|------|--------|-------------|
| **1. Diagnose** | Identify error type and DLQ volume using logs or metrics. |
| **2. Validate** | Inspect sample DLQ messages, verify format and schema. |
| **3. Fix & Redeploy** | Patch the job and confirm new version runs cleanly. |
| **4. Run Replay DAG** | Replay validated messages back into the source topic. |
| **5. Monitor** | Track replay throughput, DLQ growth, and sink latency. |
| **6. Verify & Cleanup** | Confirm all records processed; clean DLQ if resolved. |

---

## 7. Replay DAG Example (Conceptual)

| Operator | Purpose | Notes |
|-----------|----------|-------|
| `DlqValidateOperator` | Validates message structure and schema | Fails early on invalid payloads |
| `KafkaReplayOperator` | Re-ingests messages to source topic | Batch replay with offset control |
| `ReplayAuditOperator` | Records replay metrics and metadata | Logs to S3 or internal DB |

---

## Summary

DLQ replay is a **manual, controlled operation** designed for safe recovery.  
All replays should be performed after validation and monitoring setup,  
preferably through Airflow or batch workflows rather than direct Kafka commands.
