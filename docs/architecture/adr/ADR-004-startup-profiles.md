# ADR-004: Resource-Constrained Execution Profiling Under 8 GB RAM Limit

## Status
Accepted

## Date
2026-09-26

## Context
Deploying an enterprise-grade modern data architecture typically requires significant infrastructure resources. A complete platform containing PostgreSQL, Apache Kafka, MinIO, Apache Polaris, Marquez, Dagster, Prometheus, Grafana, Apache Spark, Apache Flink, and Trino would easily exceed 24–32 GB of RAM in an unconstrained production environment.

However, a core architectural constraint of this POC is **Constraint RC-02**: the entire system must be reproducible and executable on a standard developer workstation or laptop with **no more than 8,192 MB (8 GB) of total system RAM allocated to Docker/Kubernetes**.

Running all services and heavy distributed compute engines (Spark and Flink) concurrently within 8 GB RAM leads to immediate memory exhaustion, heavy swap thrashing, or catastrophic termination via the Linux Out-Of-Memory (OOM) killer.

## Decision
We implement a **modular operational profiling strategy** using Helm values overlays (`infra/profiles/*.yaml`, applied by `scripts/profile-up.sh <profile>` with reverted by `scripts/profile-down.sh`). The architecture establishes four distinct operational profiles, enforcing per-container requests/limits and sequencing resource-heavy compute engines. (Historical note: earlier drafts described Docker Compose profiles with `mem_limit`; the Helm/K8s profile set is the deployment target — there is no Compose file in this repo.)

### 1. Operational Execution Profiles (RF-23)

1. **`platform` Profile (Core Foundations — `targetRamMb: 3072` per `infra/profiles/platform.yaml`)**:
   - Deploys exactly what `scripts/profile-up.sh platform` installs: charts `s1-ingestion` (PostgreSQL), `s2-lakehouse` (MinIO + Polaris), `s7-fabric` (Marquez).
   - Keeps compute engines (Spark and Flink) idle or stopped.
   - Used for interactive SQL exploration, catalog discovery, contract authoring, and dashboard inspection.
   - Honesty note (docs follow code): `infra/profiles/platform.yaml` also carries `s6-mesh`/`s4-kafka` values entries, but the `platform` branch of the script does **not** install the `s6-mesh`/`s8-ops` charts — those deploy only under the `demo` profile (DEFERRED for the platform branch).

2. **`stream` Profile (Real-Time Ingestion & Processing - 6.0 GB RAM)**:
   - Keeps `platform` core services running.
   - Activates Apache Flink JobManager (`512M`), Flink TaskManager (`1024M`), and the Kafka Event Producer (`128M`).
   - Leaves Spark batch engines stopped.
   - Focuses on continuous clickstream consumption, sessionization, and 5-minute windowed Iceberg table commits.

3. **`batch` Profile (Medallion Batch Transformations - 7.4 GB RAM)**:
   - Keeps `platform` core services running.
   - Stops Flink streaming jobs to liberate ~1.6 GB of RAM.
   - Launches Apache Spark Driver (`1536M`) and Spark Executor (`1536M`).
   - Executes batch pipelines sequentially: `bronze_ingestion.py` -> `silver_transform.py` -> `gold_aggregation.py`.

4. **`demo` Profile (Automated Sequential End-to-End Orchestrator)**:
   - Managed automatically by `./scripts/demo.sh`.
   - Runs contracts validation, the pure-Python Medallion chain to Gold, event-log streaming with tumbling windows, fail-closed quality gates, the unified serving query, and the automated test suite (local profile; K8s engines are the deployment target, not the executor).

```
> Honesty note: the table below is the original pre-Helm sizing sketch (Docker-Compose era). The enforced source of truth is `infra/profiles/*.yaml` (`targetRamMb`, per-service requests/limits) consumed by `scripts/profile-up.sh`; K8s is the deployment target.
+-----------------------------------------------------------------------------------------+
|                               MEMORY PROFILE DISTRIBUTIONS                              |
+--------------------------+--------------------+-------------------+---------------------+
| Container Service        | Profile: platform  | Profile: stream   | Profile: batch      |
+--------------------------+--------------------+-------------------+---------------------+
| PostgreSQL 16            | 256 MB             | 256 MB            | 256 MB              |
| Apache Kafka (KRaft)     | 512 MB             | 512 MB            | 512 MB              |
| MinIO Storage            | 256 MB             | 256 MB            | 256 MB              |
| Apache Polaris Catalog   | 512 MB             | 512 MB            | 512 MB              |
| Marquez & OpenLineage    | 512 MB             | 512 MB            | 512 MB              |
| Trino Query Engine       | 1,536 MB           | 1,536 MB          | 1,536 MB            |
| Dagster Web & Daemon     | 512 MB             | 512 MB            | 512 MB              |
| Telemetry (Prom/Grafana) | 384 MB             | 384 MB            | 384 MB              |
| Flink Streaming Engine   | - (stopped)        | 1,536 MB          | - (stopped)         |
| Event Producer           | - (stopped)        | 128 MB            | - (stopped)         |
| Spark Batch Engine       | - (stopped)        | - (stopped)       | 3,072 MB            |
+--------------------------+--------------------+-------------------+---------------------+
| TOTAL ACTIVE RAM         | 4,480 MB (4.4 GB)  | 6,144 MB (6.0 GB) | 7,552 MB (7.4 GB)   |
| HEADROOM (Under 8 GB)    | 3,712 MB (3.6 GB)  | 2,048 MB (2.0 GB) | 640 MB (0.6 GB)     |
+--------------------------+--------------------+-------------------+---------------------+
```

### 2. JVM Heap and Container Tuning
- **Trino**: Configured with `-Xmx1024m` JVM heap and `1536M` container limit, sufficient for single-node vectorized execution of 200,000 records.
- **Spark**: Configured with `spark.driver.memory=1g` and `spark.executor.memory=1g`.
- **Kafka**: Operates in KRaft mode, eliminating ZooKeeper's JVM memory footprint.

## Consequences

### Positive
- **Guaranteed Stability**: Eliminates OOM crashes and kernel thrashing on 8 GB and 16 GB developer machines.
- **Cost-Free Local Execution**: Eliminates the requirement for expensive multi-node cloud clusters during POC development, testing, and portfolio evaluation.
- **Modular Workflow**: Developers can run only the specific subsystem under active development.

### Negative
- **Sequential Execution**: Real-time Flink streaming and heavy Spark batch transformations cannot run concurrently at full scale on an 8 GB machine; they must be tested sequentially or with scaled-down concurrency.
- **Profile Switching**: Operators must use profile flags (e.g., `scripts/profile-up.sh stream`, i.e. Helm `-f infra/profiles/stream.yaml`) when testing specific components.

## Alternatives Considered
- **Cloud-Only Sandbox (AWS/GCP)**: Rejected because it violates Constraint RC-01 (100% open source and local independence) and introduces cloud billing friction.
- **Micro-services Consolidation (SQLite / DuckDB)**: Rejected because replacing distributed engines (Spark, Flink, Trino) with single-node in-process databases would compromise the architectural fidelity of the enterprise reference architecture.

## Compliance & Verification
- **Functional Requirements**: Satisfies RF-23, RF-24, and RNF-07.
- **Verification**: DEFERRED on K8s — `scripts/demo.sh` runs the local Python flow only (no containers, no `docker stats`); per-profile RAM stays a declared target in `infra/profiles/*.yaml`, measurable only on the kind cluster (`scripts/setup.sh`).
