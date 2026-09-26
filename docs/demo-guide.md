# RF-24 End-to-End Demo Execution & Verification Guide

---

## 1. Overview & Objectives

This guide provides a comprehensive, step-by-step walkthrough for executing and verifying the **Modern Data Architecture Proof of Concept (POC)**. The demonstration validates **Requirement RF-24**: an automated, end-to-end operational execution covering all three architectural planes (Organization, Processing, Operations) in under 10 minutes.

### What the Demo Validates
1. **Infrastructure Initialization**: Zero-downtime bootstrap of PostgreSQL, Apache Kafka, MinIO, Apache Polaris, Marquez, Trino, Dagster, and Prometheus/Grafana under an 8 GB RAM footprint.
2. **Synthetic Data Generation**: Seeding 1,000 customers, 50,000 orders, and 200,000 line items in PostgreSQL; streaming live clickstream events into Kafka.
3. **Medallion Lakehouse Execution**: Spark batch ingestion to Iceberg Bronze, cleansing/deduplication to Silver, and aggregated modeling with GDPR PII masking to Gold.
4. **Real-Time Stream Processing**: Flink tumbling-window ingestion from Kafka to Iceberg stream buffers.
5. **Unified Serving Layer**: Trino ANSI SQL queries seamlessly merging historical batch data with real-time stream buffers via `UNION ALL`.
6. **Data Mesh & Contracts**: Validating Bitol ODCS contracts and schema backward compatibility.
7. **Active Governance & Quality Gates**: Great Expectations validation suites halting bad data; OpenLineage emitting runtime metadata to Marquez.

---

## 2. Pre-Flight Checklist

Before launching the demonstration, ensure your host environment satisfies the following minimum system requirements:

| Component | Minimum Requirement | Recommended | Verification Command |
| :--- | :--- | :--- | :--- |
| **Operating System** | Linux, macOS, or Windows 11 (WSL2) | Ubuntu 22.04 LTS or macOS M-series | `uname -a` |
| **Docker Engine** | Version 24.0+ | Version 26.0+ | `docker --version` |
| **Docker Compose** | Version 2.20+ | Version 2.27+ | `docker compose version` |
| **Memory Allocation** | 8 GB RAM allocated to Docker | 8 GB to 12 GB RAM | Docker Desktop -> Settings -> Resources |
| **Free Disk Space** | 20 GB available storage | 30 GB SSD storage | `df -h` |
| **Python** | Version 3.10+ | Version 3.11 | `python3 --version` |
| **Network Ports** | Available ports: `5432`, `9092`, `9000`, `9001`, `8181`, `5000`, `8080`, `3000` | No conflicting local daemons | `netstat -tuln` (Linux) / `Get-NetTCPConnection` (PowerShell) |

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
*(On Windows PowerShell, you can run `./scripts/demo.ps1` or run the commands sequentially within WSL2).*

### Expected Script Output Progression
The script prints structured, color-coded status checkpoints:
```text
[INFO] [1/8] Bootstrapping Core Platform Profile (PostgreSQL, Kafka, MinIO, Polaris, Marquez)...
[INFO] [2/8] Generating Synthetic OLTP Data (1,000 clients, 50,000 orders)...
[INFO] [3/8] Launching Spark Medallion Batch Pipeline (Bronze -> Silver -> Gold)...
[INFO] [4/8] Running Great Expectations Data Quality Gates...
[PASS] Quality Gate Passed! Silver Orders score: 1.00 (Threshold: 0.95)
[INFO] [5/8] Starting Real-Time Event Producer and Flink Stream Windowing...
[INFO] [6/8] Registering Polaris Catalogs and Unified Serving Views in Trino...
[INFO] [7/8] Executing Verification Queries in Trino...
[INFO] [8/8] Harvesting OpenLineage Traces from Marquez API...
[SUCCESS] End-to-End Modern Data Architecture Demo Completed Successfully!
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
```sql
SELECT 
    order_date,
    status,
    total_amount,
    num_orders,
    source_type
FROM serving.sales.unified_revenue
ORDER BY order_date DESC, status
LIMIT 10;
```
*Expected Result*: Returns rows where recent dates (last 2 days) have `source_type = 'stream'` (from Flink) and older dates have `source_type = 'batch'` (from Spark Gold). Zero duplicate dates.

#### Query 1.2: Verify GDPR PII Masking in Gold (RF-19)
```sql
SELECT 
    id,
    masked_name,
    email_hash,
    segment,
    registration_year
FROM gold.sales.dim_customers
LIMIT 5;
```
*Expected Result*:
- `masked_name` displays only initials: `"Alice S."`, `"John M."`.
- `email_hash` contains a 64-character SHA-256 hexadecimal string: `"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"`.
- Plaintext email and phone numbers are absent.

#### Query 1.3: Cross-Domain Analytical Join (RF-12)
```sql
SELECT 
    c.name AS campaign_name,
    c.channel,
    c.budget,
    SUM(p.total_amount) AS total_attributed_revenue,
    ROUND(SUM(p.total_amount) / NULLIF(c.budget, 0), 2) AS roas
FROM gold.marketing.dim_campaigns c
JOIN silver.sales.orders p ON p.channel = c.channel
GROUP BY c.name, c.channel, c.budget
ORDER BY roas DESC;
```
*Expected Result*: Aggregates sales by marketing channel in under 2 seconds.

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
*Output*:
```text
┌────────────────────────────────────────────────────────┐
│           DATA QUALITY GATE VALIDATION REPORT          │
├──────────────────────┬─────────────┬──────────┬────────┤
│ Rule ID & Check Name │ Evaluated   │ Observed │ Status │
├──────────────────────┼─────────────┼──────────┼────────┤
│ QR-001 Completeness  │ >= 0.98     │ 1.00     │ [PASS] │
│ QR-002 PK Uniqueness │ == 1.00     │ 1.00     │ [PASS] │
│ QR-003 Amount Range  │ [0.01, 100k]│ 100.0%   │ [PASS] │
│ QR-004 Status Enum   │ Approved    │ 100.0%   │ [PASS] │
└──────────────────────┴─────────────┴──────────┴────────┘
Overall Score: 100.0% (Threshold: 95.0%) -> GATE PASSED (Exit Code: 0)
```

#### Test 3.2: Simulating a Data Contract Breach (Failing Gate)
Run the quality gate with an artificially high threshold or corrupt data flag:
```bash
python quality/gates/run_quality_gate.py \
  --suite quality/expectations/silver_orders_suite.json \
  --table silver.sales.orders \
  --threshold 1.0 \
  --simulate-corrupt-data
```
*Output*:
```text
[ERROR] Rule QR-003 Failed! Found 14 records with negative 'total_amount'.
[CRITICAL] Quality Gate Breach: Observed score 92.4% < Threshold 100.0%!
[ALERT] Emitting OpenLineage FAILED event to http://marquez:5000
[ALERT] Dispatching incident notification to webhook...
[ABORT] Pipeline progression halted. Exit code: 1
```

---

### Verification 4: Orchestration & Telemetry Dashboards

#### Dagster UI (`http://localhost:3000`)
- Navigate to **Assets** -> View the Software-Defined Asset (SDA) dependency graph.
- Inspect the latest run timeline and materialization logs for `bronze_orders`, `silver_orders`, and `gold_daily_metrics`.

#### MinIO Storage Console (`http://localhost:9001`)
- Login with credentials (`minioadmin` / `minioadmin`).
- Browse bucket `lakehouse`.
- Inspect the Iceberg directory structure:
  - `lakehouse/silver/sales/orders/metadata/`: Inspect `.metadata.json` and Avro manifest files.
  - `lakehouse/silver/sales/orders/data/`: Inspect snappy-compressed Parquet files.

#### Grafana Telemetry Dashboard (`http://localhost:3002`)
- Login (`admin` / `admin`).
- View the pre-configured **Modern Data Architecture Overview** dashboard:
  - **Memory Gauge**: Confirms total active memory remains strictly `< 8,192 MB`.
  - **Kafka Ingestion Rate**: Shows continuous 10–50 events/sec.
  - **Flink Processing Latency**: Verifies sub-second event latency.
  - **Quality Gate Success Rate**: Tracks recent validation pass/fail ratios.

---

## 5. Teardown and Cleanup

To stop the platform and release all allocated resources:
```bash
# Stop all containers and remove networks
docker compose --profile platform --profile stream --profile batch down

# Optional: To remove persistent volumes and reset all data to a clean state
docker compose --profile platform --profile stream --profile batch down -v
```
