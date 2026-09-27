#!/usr/bin/env bash
# ==============================================================================
# Modern Data Architecture POC - End-to-End Automated Demonstration (RF-24)
# Demonstrates:
#  - Hexagonal Architecture (Domain Core, Ports & Adapters)
#  - F1: Batch Ingestion (OLTP -> Bronze Lakehouse with Technical Audit Columns)
#  - F2: Streaming Ingestion (Kafka/Event Stream -> Speed Table Tumbling Windows)
#  - F3: Lambda Historical Recalculation & Gold Modeling (GDPR PII Masking)
#  - F4: Unified Serving Layer (ANSI SQL: Batch Gold + Stream Speed unified view)
#  - F5: Active Data Lineage (OpenLineage Standard Events)
#  - F6: Data Quality Gates (Great Expectations assertion validation blocking bad data)
#  - F7: Data Contracts (Bitol ODCS schema validation & BACKWARD compatibility)
# ==============================================================================
set -euo pipefail

PY=python3; command -v python3 >/dev/null 2>&1 || PY=python

GREEN='\033[0;32m'
BLUE='\033[0;34m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${BLUE}==================================================================${NC}"
echo -e "${BLUE}     MODERN DATA ARCHITECTURE POC - RF-24 END-TO-END DEMO         ${NC}"
echo -e "${BLUE}==================================================================${NC}"

# Step 1: Pre-flight Checks
echo -e "\n${YELLOW}[Step 1/8] Pre-flight Environment Checks...${NC}"
python3 --version >/dev/null 2>&1 || python --version >/dev/null 2>&1 || { echo -e "${RED}Python is required.${NC}"; exit 1; }
echo -e "${GREEN}Python runtime detected.${NC}"

# Step 2: Automated Contract & Schema Evolution Verification (RF-15)
echo -e "\n${YELLOW}[Step 2/8] Validating Data Products & Contracts across Domains (Data Mesh)...${NC}"
"$PY" governance/contract_validator.py --repo-root .
echo -e "${GREEN}Data products and contracts verified for schema and backward compatibility.${NC}"

# Step 3: Run Bronze Ingestion (S1/S2 Medallion)
echo -e "\n${YELLOW}[Step 3/8] Running Medallion Architecture: Bronze Ingestion...${NC}"
"$PY" pipelines/medallion/bronze_ingestion.py --entity all
echo -e "${GREEN}Bronze tier raw ingestion complete with technical audit metadata.${NC}"

# Step 4: Run Silver Cleansing & Quality Gates (RF-06)
echo -e "\n${YELLOW}[Step 4/8] Running Silver Layer: Deduplication & Quality Gates...${NC}"
"$PY" pipelines/medallion/silver_transform.py --entity all --threshold 0.95
echo -e "${GREEN}Silver tier conformed datasets successfully validated and promoted.${NC}"

# Step 5: Run Gold Aggregation & GDPR PII Masking (RF-05 / RF-07)
echo -e "\n${YELLOW}[Step 5/8] Running Gold Layer: Business Aggregation & GDPR Pseudonymization...${NC}"
"$PY" pipelines/medallion/gold_aggregation.py --target all
echo -e "${GREEN}Gold analytical models and GDPR-masked customer dimensions published.${NC}"

# Step 6: Real-time Event Streaming & Kappa Speed Layer (RF-09 / RF-10)
echo -e "\n${YELLOW}[Step 6/8] Producing Stream Events & Computing Tumbling Windows (Kappa S4)...${NC}"
"$PY" pipelines/stream/flink_orders_stream.py --topic sales.orders.events
echo -e "${GREEN}Real-time event stream processed and speed buffer updated.${NC}"

# Step 7: Unified Serving Layer Query Execution (RF-11)
echo -e "\n${YELLOW}[Step 7/8] Executing Live Unified Serving Query (RF-11: Batch + Stream)...${NC}"
"$PY" pipelines/serving/run_serving_query.py --limit 10
echo -e "${GREEN}RF-11 Met: Single transparent query surface merging batch historical with stream speed.${NC}"

# Step 8: Execute Automated Test Suite (unit + integration)
echo -e "\n${YELLOW}[Step 8/8] Executing Automated Test Suite (Hexagonal Architecture & SOLID)...${NC}"
"$PY" -m pytest tests -q
echo -e "${GREEN}All architectural and domain tests passed successfully (unit + integration).${NC}"

echo -e "\n${GREEN}==================================================================${NC}"
echo -e "${GREEN}   DEMO COMPLETED SUCCESSFULLY! ALL PARADIGMS VERIFIED.           ${NC}"
echo -e "${GREEN}==================================================================${NC}"
