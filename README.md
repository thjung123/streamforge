# flink-cdc-pipeline

## Overview

This repository demonstrates a complete end-to-end **real-time CDC (Change Data Capture) streaming pipeline** using **Apache Flink** and **Kafka**.

The goal is to build a production-grade streaming system step-by-step — starting from ingestion, transformation, and sink, and evolving toward stateful processing, fault tolerance, and operational automation.

<br/>
At this early stage, this README focuses solely on **how the pipeline is structured and flows**, without implementation details.  

Each subsystem (savepoint, monitoring, recovery, etc.) will be documented incrementally as the project evolves.

---

## System Architecture

    +--------------+        +------------------+        +------------------+        +----------------+
    |  Data Source | --->   |     Kafka Topic  | --->   |      Flink Job    | --->   |   Sink (DB/Cache) |
    +--------------+        +------------------+        +------------------+        +----------------+
                                  ▲                             │
                                  │                             │
                           (CDC producer)                  Stateful Processing
                                                         (RocksDB + Checkpoints)
**Data Flow Steps:**

1. **Source (CDC / Producer)**
    - Periodically pulls data from an external source (e.g., database or REST API)
    - Publishes messages to a Kafka topic (`stock-stream`, etc.)

2. **Streaming Layer (Flink)**
    - Subscribes to Kafka topics as source
    - Parses and transforms events into domain-specific models
    - Applies filtering, enrichment, or aggregation logic
    - Manages state using RocksDB backend with checkpointing

3. **Sink Layer**
    - Writes processed results into storage systems (MongoDB, Redis, etc.)
    - Ensures exactly-once delivery with transactional guarantees

---

## Configuration

This project uses a `.env` file for local development and testing.  
Copy `.env.example` to `.env` and fill in the required values:

```bash
cp src/main/resources/.env.example src/main/resources/.env.example
```
