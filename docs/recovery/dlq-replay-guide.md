# DLQ Replay Guide

This document defines the design, operational policies, and replay procedures for handling events stored in the **Dead Letter Queue (DLQ)** within the Flink CDC pipeline.  
DLQ ensures that failed events are retained safely, so they can be reprocessed manually once the root cause is fixed.

---

## 1. Purpose & Scope

The DLQ is a dedicated **Kafka topic** that stores events which failed during ingestion, processing, or sink operations.  
It ensures no data is lost and allows manual reprocessing once the root cause of the failure has been resolved.

This guide covers:
- DLQ purpose and usage policy
- Failure categories and required actions
- Replay prerequisites and decision checklist
- Step-by-step replay workflow
- Automation script usage

---

## 2. DLQ Design & Policy

- DLQ is implemented as a dedicated Kafka topic
- Events must **never** be replayed automatically — **manual intervention** is always required.
- Replay should occur **only after** the root cause has been resolved and downstream systems are ready to process messages.

---

## 3. Failure Categories & Required Actions

| Stage (Pipeline) | Type              | Meaning                              | Required Action                                         |
|------------------|-------------------|---------------------------------------|---------------------------------------------------------|
| Source (Parser)  | `PARSING_ERROR`   | Event could not be deserialized       | Fix source schema or input format, then replay         |
| Processor        | `PROCESSING_ERROR`| Transformation logic failed           | Fix business logic and redeploy, then replay           |
| Sink (Writer)    | `SINK_ERROR`      | Event failed to write downstream      | Fix sink configuration or connectivity, then replay    |

---

## 4. Replay Prerequisites

Replay must be performed **only when all of the following conditions are met**:

- All schema or deserialization issues have been fixed.
- Transformation logic bugs have been resolved.
- Downstream systems are healthy and ready to receive data.

---

## 5. Replay Workflow

Follow this sequence to prevent inconsistent state or data corruption.

### Step 1: Diagnose and Fix

- Inspect messages in the DLQ topic
- Review job logs to identify the root cause.
- Apply the necessary fixes (e.g., schema, transformation logic, or sink configuration).

### Step 2: Replay Events

Once the root cause is resolved, replay the failed events back into the original source topic using the helper script:

```bash
./scripts/replay_dlq.sh <dlq_topic> <source_topic> <bootstrap_server>
```

**Example:**
``` bash
./scripts/replay_dlq.sh cdc-dlq cdc-topic kafka:9092
```

Replay must always happen only after the underlying issue is fixed and the pipeline is ready to process events correctly.


## 6. Operational Notes

- Never replay messages if you are unsure whether the issue is fully resolved.
- Coordinate with downstream service owners before replaying a large batch of DLQ data.
- Keep track of replay attempts and message volume for audit and debugging purposes.