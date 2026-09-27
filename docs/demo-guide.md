# RF-24 End-to-End Demo Execution & Verification Guide

---

## 1. Overview & Objectives

This guide provides a comprehensive, step-by-step walkthrough for executing and verifying the **Modern Data Architecture Proof of Concept (POC)**. The demonstration validates **Requirement RF-24**: an automated, end-to-end operational execution covering all three architectural planes (Organization, Processing, Operations) in under 10 minutes.

> **Execution profile note.** The automated demo below runs the **local profile**:
> the hexagonal Python core with SQLite-backed adapters (no cluster required).
> Steps that mention Trino/Marquez/Dagster/MinIO UIs describe the **K8s profile**
> (`infra/helm`, `infra/profiles`) and are labelled as such where applicable.

### What the Demo Validates
1. **Data Products & Contracts**: validating Bitol ODCS manifests and real schema
   backward compatibility (`governance/contract_validator.py` → core use case).
2. **Medallion Lakehouse Execution**: idempotent append ingestion to Bronze with
   audit columns, fail-closed cleansing/deduplication to Silver, and aggregated
   modeling with GDPR PII masking to Gold.
3. **Real-Time Stream Processing**: 5-minute tumbling windows over the event log
   with persistent consumer cursors (no duplicates) and full-log replay.
4. **Unified Serving Layer**: one SQL statement merging exact batch Gold with
   recent stream windows via anti-join — no double counting.
5. **Active Governance & Quality Gates**: fail-closed expectation suites halt bad
   data; OpenLineage events are emitted to Marquez when reachable and always
   recorded in the local audit trail.
6. **Automated Test Suite**: unit + integration tests covering the RF acceptance
   scenarios.

---

## 2. Pre-Flight Checklist

Before launching the demonstration, ensure your host environment satisfies the following minimum system requirements:

| Component | Minimum Requirement | Recommended | Verification Command |
| :--- | :--- | :--- | :--- |
| **Operating System** | Linux, macOS, or Windows 11 (WSL2) | Ubuntu 22.04 LTS or macOS M-series | `uname -a` |
| **Python** | Version 3.11+ | Version 3.11 | `python --version` |
| **Free Disk Space** | 20 GB available storage | 30 GB SSD storage | `df -h` |
| **Docker Engine** *(K8s profile only)* | Version 24.0+ — not required for the local profile | Version 26.0+ | `docker --version` |
| **Docker Compose / kind** *(K8s profile only)* | Only needed to deploy `infra/helm` + `infra/profiles` | kind v0.20+ | `docker compose version` / `kind --version` |
| **Memory Allocation** *(K8s profile only)* | 8 GB RAM allocated to Docker | 8 GB to 12 GB RAM | Docker Desktop -> Settings -> Resources |
| **Network Ports** *(K8s profile only)* | Available ports: `5432`, `9092`, `9000`, `9001`, `8181`, `5000`, `8080`, `3000` | No conflicting local daemons | `netstat -tuln` (Linux) / `Get-NetTCPConnection` (PowerShell) |

> The local profile (default) needs **only Python 3.11+** — no containers required.
> Docker / Kubernetes / Trino / Marquez / Dagster / MinIO apply exclusively to the
> K8s profile (`infra/helm`, `infra/profiles`).

---

## 3. Automated Execution via `scripts/demo.sh`

The entire demonstration is automated via the executable script `scripts/demo.sh`.

### Step 1: Clone and Navigate to Repository Root
```bash
git clone https://github.com/sergio11/poc_data_engineering.git
cd poc_data_engineering
```

### Step 2: Grant Execution Permissions (Linux / macOS / WSL2)
```bash
chmod +x scripts/demo.sh
```

### Step 3: Execute the Automated Demo Script
```bash
./scripts/demo.sh
```
*(On Windows the same steps are available cross-platform via the Rakefile:
`rake demo`. There is no `demo.ps1`; use `rake` or Git Bash/WSL2 for the
shell script.)*

### Expected Script Output Progression
The script prints structured, color-coded status checkpoints (local profile —
pure Python + SQLite, no cluster services are started):
```text
[Step 1/8] Pre-flight Environment Checks...              -> Python runtime detected.
[Step 2/8] Validating Data Products & Contracts ...      -> verified (RF-15, exit 0).
[Step 3/8] Running Medallion Architecture: Bronze ...    -> raw ingestion with audit metadata.
[Step 4/8] Running Silver Layer: Deduplication & ...    -> validated and promoted (RF-06 gate).
[Step 5/8] Running Gold Layer: Business Aggregation ... -> models + GDPR masking published.
[Step 6/8] Producing Stream Events & Computing ...       -> speed buffer updated (RF-09/10).
[Step 7/8] Executing Live Unified Serving Query ...      -> batch + stream merged (RF-11).
[Step 8/8] Executing Automated Test Suite ...            -> unit + integration tests pass.
DEMO COMPLETED SUCCESSFULLY! ALL PARADIGMS VERIFIED.
```

---

## 4. Manual Verification & Inspection Walkthrough

After the demo script completes, perform manual verification across the user interfaces and query endpoints:

### Verification 1: Interactive Unified Queries in Trino
Open the Trino CLI or execute queries via HTTP / Python:
```bash
trino --server http://localhost:8080 --catalog iceberg --schema serving
```

#### Query 1.1: Verify Unified Batch + Stream Revenue (RF-11)
Run the unified serving query through the hexagonal core (same semantics are
also expressed as SQL in `pipelines/serving/unified_view.sql` for Trino):
```bash
python pipelines/serving/run_serving_query.py --limit 10
```
It executes a single query that merges `gold.sales.daily_metrics` (exact batch)
with `stream.sales.orders_windowed` (speed layer) using an anti-join on
`(order_date, status)`: the batch value wins on collision and stream rows only
fill keys absent from batch — so no key is ever counted twice.
*Expected Result*: rows labelled `source_tier = 'batch_historical'` (exact,
older) and `source_tier = 'stream_speed'` (recent windows). Zero duplicate
`(order_date, status)` keys.

#### Query 1.2: Verify GDPR PII Masking in Gold (RF-05, RNF-07)
Query the customer dimension materialised by `AggregateGoldUseCase`
(`src/domain/privacy.py` enforces the masking policy):
```sql
SELECT
    id,
    name_masked,
    email_hash,
    segment
FROM gold.sales.customers
LIMIT 5;
```
*Expected Result*:
- `name_masked` displays only first name + initial: `"Alice S."`, `"John M."`.
- `email_hash` contains a 64-character SHA-256 hexadecimal string of the
  normalised email.
- Plaintext `name` and `email` columns are absent from the Gold table.

#### Query 1.3: Pseudonymised Customer × Orders Join (RNF-07)
Demonstrates that analytics still work on masked data — join the pseudonymised
Gold customer dimension with Silver orders:
```sql
SELECT
    c.id AS customer_id,
    c.name_masked,
    c.segment,
    COUNT(o.id) AS total_orders,
    ROUND(SUM(CAST(o.total_amount AS REAL)), 2) AS total_spent
FROM gold.sales.customers c
JOIN silver.sales.orders o ON o.customer_id = c.id
GROUP BY c.id, c.name_masked, c.segment
ORDER BY total_spent DESC
LIMIT 10;
```
*Expected Result*: per-customer order counts and spend computed without ever
exposing a plaintext email or full name. (Marketing/Finance cross-domain Gold
tables are PLANNED, not produced in this POC.)

---

### Verification 2: Active Lineage in Marquez UI
1. Open your web browser and navigate to: **`http://localhost:3001`** (or `http://localhost:5000`).
2. Navigate to the **Namespaces** dropdown and select `default` or `iceberg`.
3. Select the job **`silver_transform`** or dataset **`silver.sales.orders`**.
4. **Visual Inspection**:
   - Verify the directed acyclic graph (DAG) shows lineage from `postgresql.public.orders` -> `bronze_ingestion` -> `bronze.sales.orders` -> `silver_transform` -> `silver.sales.orders` -> `gold_aggregation` -> `gold.sales.daily_metrics`.
   - Inspect dataset facets to verify the schema and column-level types captured by OpenLineage.

---

### Verification 3: Data Quality Gates & Alert Testing
Verify how the platform handles data quality gate checks and prevents bad data from reaching production:

#### Test 3.1: Normal Quality Gate Run (Passing)
```bash
python quality/gates/run_quality_gate.py \
  --suite quality/expectations/silver_orders_suite.json \
  --table silver.sales.orders \
  --threshold 0.95
```
*Output* (real run against the current `data/` state, trimmed — rich-table
borders omitted):
```text
Expectation Suite Evaluation: silver_orders_suite
| Rule / Assertion       | Target Column | Status | Details                   |
| All Suite Expectations | *             | PASS   | 100% assertions satisfied |

Quality Gate Summary
  Table: silver.sales.orders
  Suite: silver_orders_suite
  Passed: 8 / 8 assertions
  Pass Rate: 100.0% (Threshold: 95.0%)
  Gate Verdict: PASSED
(exit code 0)
```

#### Test 3.2: Strict-threshold run (same data, threshold 1.0)
Re-run the gate with a strict 100% threshold (there is no
`--simulate-corrupt-data` flag — the CLI only accepts `--suite`, `--table`,
`--threshold`, plus optional `--input-file`/`--marquez-url`/`--output-dir` options):
```bash
python quality/gates/run_quality_gate.py \
  --suite quality/expectations/silver_orders_suite.json \
  --table silver.sales.orders \
  --threshold 1.0
```
*Output* (real run against the current `data/` state, trimmed):
```text
Expectation Suite Evaluation: silver_orders_suite
| Rule / Assertion       | Target Column | Status | Details                   |
| All Suite Expectations | *             | PASS   | 100% assertions satisfied |

Quality Gate Summary
  Table: silver.sales.orders
  Suite: silver_orders_suite
  Passed: 8 / 8 assertions
  Pass Rate: 100.0% (Threshold: 100.0%)
  Gate Verdict: PASSED
(exit code 0)
```
On the current clean data the strict run also passes. A real breach (e.g. null
primary keys, see `test_rf06_*` in `tests/integration/test_rf_acceptance.py`)
fails the gate with exit code 1, emits a lineage `FAIL` event when a
Marquez/webhook URL is configured, and blocks pipeline progression.

---

### Verification 4: Orchestration & Telemetry Dashboards (K8s profile)

> The UIs below exist only when the K8s profile is deployed
> (`infra/helm`, `infra/profiles`). The local-profile demo produces the same
> functional evidence via the CLI steps and `data/lineage_audit.jsonl`.

#### Dagster UI (`http://localhost:3000`)
- Navigate to **Assets** -> View the Software-Defined Asset (SDA) dependency graph.
- Inspect the latest run timeline and materialization logs for the medallion assets.

#### MinIO Storage Console (`http://localhost:9001`)
- Browse bucket `lakehouse` and inspect the Iceberg table metadata/data layout.

#### Grafana Telemetry Dashboard (`http://localhost:3002`)
- View the **Modern Data Architecture Overview** dashboard (memory, ingestion
  rate, processing latency, quality-gate pass/fail ratios).

---

## 5. Teardown and Cleanup

Local profile: the demo writes only to `data/` (git-ignored SQLite files and the
lineage audit log). To reset to a clean state:
```bash
# Windows PowerShell
Remove-Item data/*.db, data/*.jsonl -ErrorAction SilentlyContinue

# Linux / macOS / WSL2
rm -f data/*.db data/*.jsonl
```
K8s profile: stop the deployed stack with the profile scripts
(`scripts/profile-down.sh`) or your cluster teardown of choice.
