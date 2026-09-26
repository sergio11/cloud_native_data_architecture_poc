# ADR-003: Multi-Domain Data Mesh with Data Products and Declarative Contracts

## Status
Accepted

## Date
2026-09-26

## Context
Centralized monolithic data architectures (such as centralized data warehouses or central data lake teams) create severe organizational bottlenecks as organizations scale:
1. **Domain Disconnect**: Central data teams lack the deep contextual knowledge required to understand source business logic, resulting in misinterpreted data and slow pipeline development.
2. **Schema Drift & Broken Downstream Consumers**: Upstream application developers frequently modify database schemas without communicating with data consumers, breaking analytics pipelines silently.
3. **Lack of Operational Accountability**: Data quality is treated as an afterthought; when pipelines fail, it is unclear whether the data producer or data consumer is responsible for remediation.

To eliminate these failure modes, we adopt the principles of **Data Mesh**: domain ownership, data as a product, self-serve data infrastructure, and federated computational governance.

## Decision
We organize the data architecture around three autonomous business domains (**Ventas**, **Marketing**, **Finanzas**) and mandate that all published data assets be encapsulated as **Data Products** governed by formal, declarative **Data Contracts**.

### Architectural Implementation

#### 1. Domain Decentralization (RF-13)
The codebase and data assets are segregated strictly by domain boundaries:
- `domains/ventas`: Owns customer transactions, orders (`pedidos`), line items, and fulfillment lifecycles.
- `domains/marketing`: Owns multi-channel campaigns (`campanas`), lead generation (`leads`), and web clickstream telemetry (`navegacion`).
- `domains/finanzas`: Owns revenue recognition models, margin aggregations, and cross-domain financial reconciliation.

#### 2. Data Product Encapsulation (RF-14)
Each published analytical dataset is modeled as an autonomous Data Product with a standardized directory layout and descriptor (`product.yaml`):
```
domains/ventas/data-products/pedidos/
├── product.yaml        # Manifest: ownership, metadata, SLA, output ports
├── contract.yaml       # Formal Data Contract (Bitol ODCS v2)
└── schema.json         # JSON Schema Draft-07 specification
```

#### 3. Declarative Data Contracts (RF-15, RF-16)
Every data product must maintain an authoritative `contract.yaml` compliant with the Bitol Open Data Contract Standard (ODCS). Key elements include:
- **Authoritative Schema**: Field names, datatypes, constraints (minimums, regex, enum lists), and privacy classification (`confidential`, `internal`).
- **Quality Rules (SLAs)**: Automated assertions specifying minimum completeness (e.g., `>= 0.98`), primary key uniqueness (`1.0`), and range validity.
- **Compatibility Policy**: Mandates `BACKWARD` compatibility. Producers are forbidden from dropping fields, renaming columns, or narrowing datatypes without a major version increment and a 90-day deprecation notice.
- **Consumer Registration**: Explicitly lists downstream consumers, their intended use cases, and approved assets, establishing an auditable chain of custody.

#### 4. Federated Computational Governance (RF-19, RF-20)
- Global standards (e.g., GDPR Article 25 compliance, SHA-256 PII masking for emails, unprivileged container execution) are defined centrally by data stewards.
- Local execution of these rules is automated within domain pipelines (`gold_aggregation.py`) and verified by automated quality gates (`run_quality_gate.py`).

```
 +-------------------------------------------------------------------------------+
 |                               DOMAIN: VENTAS                                  |
 |  Product: pedidos                                                             |
 |  - Manifest: product.yaml (Owner: equipo-ventas@empresa.com)                  |
 |  - Contract: contract.yaml (v1.2.0, BACKWARD compatibility, PII tags)         |
 |  - Schema:   schema.json (Draft-07)                                           |
 |  - Output:   Iceberg table: silver.ventas.pedidos                             |
 +-------------------------------------------------------------------------------+
         |
         | Formal Data Contract Agreement (Bitol ODCS)
         v
 +-------------------------------------------------------------------------------+
 |                               CONSUMER DOMAINS                                |
 |  - Finanzas: Consumes gold_pedidos for P&L revenue calculations               |
 |  - Marketing: Consumes silver_pedidos for customer attribution & ROI          |
 +-------------------------------------------------------------------------------+
```

## Consequences

### Positive
- **Clear Domain Accountability**: Domain owners are directly accountable for the quality, freshness, and schema stability of their data products.
- **Elimination of Silent Failures**: Downstream consumers are protected by declarative contracts; breaking changes are caught at CI/test time rather than in production.
- **Self-Documenting Ecosystem**: Manifests and contracts provide machine-readable and human-readable metadata, simplifying onboarding and catalog discovery.
- **Regulatory Compliance**: Privacy classifications within contracts ensure PII masking is systematically verified.

### Negative
- **Initial Authoring Overhead**: Domain developers must author and maintain YAML contracts and JSON schemas in addition to transformation code.
- **Strict Governance Discipline**: Schema modifications require formal version management and deprecation periods.

## Alternatives Considered
- **Centralized Monolithic Data Warehouse**: Rejected because it recreates the centralized team bottleneck, slow release cycles, and domain disconnect.
- **Uncontracted Data Lake**: Rejected because raw unmanaged tables lack quality guarantees, schema evolution policies, and consumer tracking, leading to frequent pipeline breakages.

## Compliance & Verification
- **Functional Requirements**: Satisfies RF-13, RF-14, RF-15, RF-16, and RF-19.
- **Verification**: Verified via schema inspection in `domains/ventas/data-products/pedidos/contract.yaml` and quality gate validation in `quality/gates/run_quality_gate.py`.
