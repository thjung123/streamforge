# flink-cdc-pipeline

## Overview

This repository demonstrates a complete end-to-end **real-time CDC (Change Data Capture) streaming pipeline** using **Apache Flink** and **Kafka**.

The goal is to build a production-grade streaming system step-by-step — starting from ingestion, transformation, and sink, and evolving toward stateful processing, fault tolerance, and operational automation.

<br/>
At this early stage, this README focuses solely on **how the pipeline is structured and flows**, without implementation details.  

Each subsystem (savepoint, monitoring, recovery, etc.) will be documented incrementally as the project evolves.

---

## System Architecture

    +--------------+        +------------------+        +----------------+
    |  Data Source | --->   |   Processing     | --->   |     Sink       |
    +--------------+        +------------------+        +----------------+
**Data Flow Steps:**

1. **Source Layer**
   - Continuously ingests change events from external systems (e.g., databases, REST APIs, or existing Kafka topics)
   - Supports various CDC connectors or streaming producers for real-time data ingestion
2. **Processing Layer (Flink)**
   - Consumes streaming events from supported sources
   - Parses and transforms records into domain-specific models
   - Applies filtering, enrichment, windowing, or aggregation logic
   - Maintains application state with checkpointing for fault tolerance
3. **Sink Layer**
   - Delivers processed results into downstream systems (e.g., MongoDB, Redis, Elasticsearch, or Kafka topics)
   - Supports delivery guarantees (e.g., **exactly-once**) and ensures transactional consistency

---

## Configuration

The project uses a single `.env` file for local development, testing, and as a default configuration source.
This file contains baseline settings used when no external configuration is provided.

In real deployments (e.g. staging or production), values in `.env` are typically overridden by environment variables or JVM system properties provided by the deployment platform (such as Kubernetes, Docker, or a secrets manager like Vault).
