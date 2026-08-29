# Implementation Plan: MVP Google Cloud Pub/Sub Enterprise Architecture Demo

## Phase 1: Core Foundation & Proto-First Schemas (TDD)
- [x] Task: Project Setup & Dependencies (1cad93f)
  - [x] Define `pyproject.toml` and `requirements.txt`
  - [x] Initialize project directory layout (`src/`, `tests/`, `proto/`, `scripts/`)
- [x] Task: Proto-First Schema & Code Generation (7642624)
  - [x] Write `proto/streaming_event.proto` defining `StreamingEvent`
  - [x] Write unit tests for proto serialization/deserialization
  - [x] Implement proto compilation script
- [x] Task: Zstandard Compression & Decompression Layer (e0ca68c)
  - [x] Write unit tests for zstd compression ratio and transparency (`tests/test_compression.py`)
  - [x] Implement `CompressionManager` in `src/compression.py`
- [x] Task: Phase Verification & Checkpoint (Refer to workflow.md) (37f3351)

## Phase 2: Dual-Path Ingestion Engine (TDD)
- [x] Task: Mock & Local Abstraction Layer (aa3f082)
  - [x] Write tests for Pub/Sub & Storage mock interfaces
  - [x] Implement mock GCP client wrapper (`src/gcp_client.py`) supporting real GCP (`pub-sub-kamo`) and offline local mode
- [x] Task: Dual-Path Publisher (Fast Path vs GCS Offload) (e070731)
  - [x] Write tests for payload routing (< 8MB vs >= 8MB) (`tests/test_publisher.py`)
  - [x] Implement `DualPathPublisher` in `src/publisher.py`
- [x] Task: Dual-Path Ingestion Consumer (665ff75)
  - [x] Write tests for consumer payload reconstitution (`tests/test_consumer.py`)
  - [x] Implement `DualPathConsumer` in `src/consumer.py` (inline decompress vs GCS blob fetch)
- [x] Task: Phase Verification & Checkpoint (Refer to workflow.md) (a1243d5)

## Phase 3: StreamingPull vs Synchronous Pull Benchmark (TDD)
- [x] Task: Synchronous Pull Worker (Legacy Baseline) (924b562)
  - [x] Write tests for synchronous pull worker with long-polling timeout simulation
  - [x] Implement `SyncPullWorker` in `src/workers/sync_worker.py` (measuring idle wait overhead)
- [x] Task: gRPC StreamingPull Worker (Optimized Pipeline) (ba13a40)
  - [x] Write tests for StreamingPull worker with asynchronous message handling
  - [x] Implement `StreamingPullWorker` in `src/workers/streaming_worker.py` (persistent bidirectional gRPC)
- [ ] Task: Benchmark & Metrics Collector
  - [ ] Write tests for metrics aggregation (P50/P95/P99 latency, CPU utilization ratio)
  - [ ] Implement `MetricsCollector` in `src/metrics.py`
- [ ] Task: Phase Verification & Checkpoint (Refer to workflow.md)

## Phase 4: GCP Infrastructure Automation & BigQuery Zero-ETL
- [ ] Task: Infrastructure Provisioning Script (`scripts/setup_infra.py`)
  - [ ] Write script to provision Pub/Sub topics, subscriptions, DLQ, GCS bucket, BigQuery dataset/table
  - [ ] Configure Pub/Sub Push subscription to BigQuery Zero-ETL
- [ ] Task: Infrastructure Teardown Script (`scripts/cleanup_infra.py`)
  - [ ] Write safe cleanup script to delete created GCP demo resources on demand
- [ ] Task: Dead Letter Queue (DLQ) & Error Injection Flow
  - [ ] Write integration test for corrupt payload injection and 5-retry DLQ routing
  - [ ] Implement DLQ handler and verifier in `src/dlq.py`
- [ ] Task: Phase Verification & Checkpoint (Refer to workflow.md)

## Phase 5: Streamlit Interactive Web Dashboard & Demo Runner
- [ ] Task: Synthetic Workload Generator
  - [ ] Implement configurable traffic generator (rate, size distribution, error toggle) in `src/generator.py`
- [ ] Task: Streamlit Dashboard UI
  - [ ] Build interactive controls (Rate slider, Large Payload %, Error Injection button)
  - [ ] Build live metrics visualization (Dual-path counters, Sync vs StreamingPull latency bar charts)
  - [ ] Build BigQuery streaming data viewer and DLQ monitoring panel
- [ ] Task: End-to-End Demo Script & Documentation
  - [ ] Create `run_demo.sh` for one-click startup (offline mock or live GCP)
  - [ ] Update `README.md` with demo presentation guide, architecture diagrams, and talking points
- [ ] Task: Phase Verification & Checkpoint (Refer to workflow.md)
