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

## Kafka as the Central Durable Layer

All pipelines in this repository are designed around Kafka as the central data backbone.
This is a deliberate design choice for durability, observability, and operational resilience.


#### Rationale:
- High Durability & Replayability
  - Kafka persists all CDC events durably, allowing downstream jobs to replay or reprocess data deterministically in case of failure or schema evolution.
- Decoupling of Upstream and Downstream
  - By routing every change event through Kafka, the system naturally separates the data ingestion flow (Mongo → Kafka) from the data delivery flow (Kafka → Target DB).
  - This prevents cascading failures and enables independent scaling or redeployment of each job. 
- Fault Isolation & Recovery
  - If a downstream sink fails or needs maintenance, upstream pipelines can continue running — events remain in Kafka until consumption resumes.
  - Combined with Flink checkpointing, this provides end-to-end fault tolerance.
- Unified CDC Format
 - Every event published to Kafka follows a standardized envelope (CdcEnvelop), containing metadata such as operation type, event time, and trace ID.
 - This ensures consistent parsing and monitoring across all domains.

Canonical pipeline pattern:
```
Source (CDC) → Parser → Processor → Kafka
Kafka → Parser → Processor → Sink (DB, Redis, etc.)
```

Kafka thus serves both as:
- Sink for upstream CDC ingestion jobs, and
- Source for downstream delivery or analytical pipelines.

This architecture scales well across multiple teams and domains, allowing new sinks or processors to be added without impacting upstream data producers.

---


## Configuration

The project uses a single `.env` file for local development, testing, and as a default configuration source.
This file contains baseline settings used when no external configuration is provided.

In real deployments (e.g. staging or production), values in `.env` are typically overridden by environment variables or JVM system properties provided by the deployment platform (such as Kubernetes, Docker, or a secrets manager like Vault).

## Running Locally

This project is designed primarily to demonstrate **architecture, design patterns, and production-grade pipeline structure**, rather than to provide a runnable demo.

However, you can still run it locally for experimentation:

```bash
docker-compose up -d       # Start Kafka, MongoDB, and other dependencies
java -jar build/libs/app-all.jar MongoToKafka
```


## Deployment

Deployment manifests (e.g., Kubernetes `FlinkDeployment` resources, Helm charts) are not included in this repository,  
as the focus is on the **application design and CDC pipeline structure**.

In a production environment, the pipeline would typically be deployed as a Flink Application on Kubernetes,  
integrated with a configuration provider (e.g., Vault), and monitored via Prometheus/Grafana.