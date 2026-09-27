# RF Traceability Status — what is actually verified

> Source of truth for requirements: `srs.txt` (§5 functional, §6 non-functional,
> §9 acceptance, §10 paradigm traceability).
> Rule used in this report: a requirement is **VERIFIED** only if a concrete
> automated test or command in this repository proves it. Everything else is
> marked **PARTIAL**, **DEFERRED** (K8s profile), or **OUT OF SCOPE** — with the
> reason stated. Status counts at the bottom.

Evidence commands:
- `python -m pytest tests -q` — full suite (64 tests: 54 unit + 10 integration)
- `python -m pytest tests/integration -q` — RF acceptance scenarios
- `rake demo` — RF-24 end-to-end (local profile: Python + SQLite, no cluster)

---

## Functional requirements (RF-01 … RF-25)

| RF | Requisito (srs.txt) | Estado | Evidencia |
|---|---|---|---|
| RF-01 [M] | Ingesta OLTP periódica e idempotente a Bronze | **VERIFIED** | `test_ingest_idempotency.py::test_ingest_bronze_idempotent`, `test_storage_time_travel.py::test_append_idempotent` (re-run writes 0 rows) |
| RF-02 [M] | Eventos de dominio en el log con timestamp y procedencia | **VERIFIED** | `test_stream_processing.py` (publish/consume round-trip), `test_end_to_end.py::test_full_demo_flow` |
| RF-03 [M] | Separación estricta Bronze → Silver → Gold, sin saltos | **VERIFIED** | `test_use_cases.py::test_medallion_pipeline_flow` (cada tier leído y verificado) |
| RF-04 [M] | Silver: limpieza, tipado, deduplicación, validaciones | **PARTIAL** | Deduplicación + validación: VERIFIED (`test_quality_gate.py`, suites `silver_*_suite.json`). **Tipado explícito de columnas NO implementado** (todo se almacena como TEXT; las conversiones son tolerantes en lectura). |
| RF-05 [M] | Gold: métricas/agregados de negocio listos para consumo | **VERIFIED** | `test_domain_entities.py::test_business_aggregator_daily_sales`, `test_medallion_pipeline_flow` (`gross_revenue`, `average_order_value`) |
| RF-06 [M] | Quality gates bloquean promoción + notifican al dueño | **VERIFIED** | `test_rf_acceptance.py::test_rf06_null_breach_blocks_silver_and_alerts_via_lineage` (15% nulos ⇒ `QualityGateBreachError` + evento lineage `FAIL`, nada en Silver); suite ausente / tipo desconocido / vacío también bloquean |
| RF-07 [M] | ACID, time travel, evolución de esquema | **VERIFIED** | `test_rf_acceptance.py::test_rf07_time_travel_returns_previous_gold_version_unchanged`, `test_schema_evolution`, `test_legacy_table_compatibility` (equivalente local de snapshots Iceberg; ver `LakehouseStorageAdapter`) |
| RF-08 [M] | Batch: recálculo completo del histórico | **VERIFIED** | Por diseño: Gold recomputa íntegramente desde Silver en cada ejecución (`AggregateGoldUseCase`, sin flag incremental); cubierto e2e por `test_full_demo_flow` |
| RF-09 [M] | Stream: agregados en ventana, latencia ≤ 5 s | **PARTIAL** | Ventanas tumbling reales de 5 min e inmediatez: VERIFIED (`test_rf09_five_minute_windows_reflect_events_immediately`). El límite de latencia ≤ 5 s **no se mide**: el speed layer es un CLI por polling, no un demonio residente. |
| RF-10 [M] | Kappa: replay releyendo el log, sin batch paralelo | **VERIFIED** | `test_rf10_replay_is_deterministic_and_ignores_consumer_cursor` (replay total determinista, no avanza cursores) |
| RF-11 [M] | Serving: una consulta fusiona batch + stream | **VERIFIED** | `test_rf11_batch_wins_and_stream_fills_gaps_in_single_query`, `test_serving_unified.py::test_batch_wins_no_double_count` (anti-join: batch gana, sin doble conteo) |
| RF-12 [S] | Reproceso completo de cualquier capa sin inconsistencia | **PARTIAL** | Replay de stream + recomputación Gold + lecturas versionadas: VERIFIED. Re-ejecución medallion completa sin duplicados: VERIFIED (`test_full_demo_flow`). Sin comando único de reproceso Bronze→Gold en una invocación. |
| RF-13 [M] | Publicación declarativa de productos por dominio | **PARTIAL** | Los 6 productos existen como manifiestos declarativos (`domains/**/product.yaml`) y validan sin tocar el núcleo (`test_valid_product_set_passes`). No hay pipeline de "publicación" con registro: el manifiesto **es** la publicación en el perfil local. |
| RF-14 [M] | Productos encontrables y autoexplicativos en catálogo | **PARTIAL** | Manifiestos autoexplicativos (dueño, SLA, clasificación, esquema, linaje declarado): sí. Catálogo buscable con UI: solo perfil K8s (Marquez); en local, el catálogo es el árbol `domains/` + validación. |
| RF-15 [M] | Contratos máquina-legibles validados; ruptura bloquea | **VERIFIED** | `test_rf15_governance_cli_blocks_field_rename_and_passes_clean_tree` + 15 tests en `test_contract_validation.py` (eliminación/cambio de tipo/campo requerido ⇒ bloqueo, exit 1). Nota: el bloqueo actúa en ejecución/validación; **no hay pipeline CI** que lo imponga en merge). |
| RF-16 [M] | Consumo cross-dominio self-service | **PARTIAL** | Cualquier consumidor puede consultar el serving sin intermediación (`test_rf11`, `test_full_demo_flow`). Sin capa de control de acceso: es apertura total, no self-service gobernado. |
| RF-17 [S] | Políticas globales federadas aplicadas automáticamente | **PARTIAL** | Dueño/clasificación/nombres/modo de compatibilidad se exigen en validación (`test_broken_product_missing_owner_reported`). Admisión en despliegue (Kyverno): solo values, sin templates — DEFERRED a K8s. |
| RF-18 [S] | Cuotas/aislamiento por dominio | **DEFERRED** | `ResourceQuota`/`LimitRange` por dominio existen en el chart `s6-mesh` (perfil K8s). Sin enforcement en perfil local. |
| RF-19 [M] | Linaje end-to-end consultable, alimentado automáticamente | **VERIFIED** | `test_rf19_lineage_records_connected_bronze_to_gold_chain` (cadena bronze→silver→gold conectada en `data/lineage_audit.jsonl`; grafo UI solo en K8s/Marquez). |
| RF-20 [S] | Metadatos activos: alertas ante cambios/degradación | **PARTIAL** | `lineage/config/lineage_webhook_service.py` + `alert-rules.yaml` existen y son desplegables vía `s7-fabric`, pero **no forman parte del flujo `rake demo` ni tienen tests**. |
| RF-21 [S] | Seguridad/clasificación embebida en el flujo | **PARTIAL** | Pseudonimización PII embebida en el flujo Gold: VERIFIED (`test_gdpr_privacy_policy`, columnas `email_hash`/`name_masked` en `test_full_demo_flow`). Sin RBAC en tiempo de ejecución. |
| RF-22 [M] | Pipelines orquestables con estado observable | **VERIFIED** | Tareas `rake` observables bajo demanda + definiciones Dagster + exit codes 0/1/2 en gates. |
| RF-23 [S] | Panel único (pipelines, lag, calidad) | **DEFERRED** | Assets de Grafana/Prometheus en `infra/helm/s8-ops`, pero **el chart no tiene templates**: ningún dashboard desplegado. |
| RF-24 [M] | Comando único de demo reproducible | **VERIFIED** | `rake demo` (y `scripts/demo.sh`): ingesta → producto → consumo → tests; flujo F1–F6 cubierto e2e por `test_full_demo_flow`. |
| RF-25 [M] | Documentación de arquitectura apta para portfolio | **VERIFIED** | `docs/architecture/overview.md` (perfil local vs K8s), ADRs, `docs/concepts-to-components.md`, este informe. |

**Counts — RF:** VERIFIED **14** · PARTIAL **9** · DEFERRED **2** · OUT OF SCOPE **0**

---

## Non-functional requirements (RNF-01 … RNF-11)

| RNF | Requisito (srs.txt) | Estado | Evidencia / nota |
|---|---|---|---|
| RNF-01 | Operar en 8 GB mediante perfiles exclusivos | **PARTIAL** | Perfiles declarados en `infra/profiles/*.yaml` (K8s). Perfil local: un proceso Python + SQLite (decenas de MB). Sin medición de consumo real. |
| RNF-02 | Serving < 3 s; stream ≤ 5 s | **PARTIAL** | Dataset demo pequeño: todo responde muy por debajo; **sin medición de latencia instrumentada**. |
| RNF-03 | Despliegue reproducible desde Git, sin pasos manuales | **PARTIAL** | `pyproject.toml` + Rakefile versionados; perfil local reproducible en Windows+Python. Sin `requirements.lock` (rangos, no pins) ni despliegue K8s probado aquí. |
| RNF-04 | Solo Windows + Podman + kind, sin cloud | **PARTIAL** | Perfil local: solo Windows + Python. Perfil K8s definido (kind), no levantado en esta verificación. |
| RNF-05 | Idempotencia de pipelines | **VERIFIED** | `test_ingest_idempotency.py`, `test_process_stream_two_runs_no_duplicate_windows`, re-ejecución e2e sin duplicados. |
| RNF-06 | Aislamiento entre dominios | **DEFERRED** | Vía cuotas K8s (`s6-mesh`); sin enforcement local. |
| RNF-07 | Sin credenciales incrustadas; PII mascarada | **VERIFIED** | Sin defaults de password en el núcleo (`PGPASSWORD` solo env; `test_oltp_source_validation`); PII mascarada en Gold (tests GDPR). Resto de secretos demo viven en `values.yaml` de Helm (perfil K8s). |
| RNF-08 | 100% Gold con linaje consultable | **VERIFIED** | Todos los use cases emiten linaje (`test_rf19…`). |
| RNF-09 | Versiones fijadas, modularidad, ADRs | **PARTIAL** | Modularidad + ADRs: sí. Versiones: rangos en `pyproject.toml`, **no pins** (se documenta como deuda menor). |
| RNF-10 | Recuperación tras reinicio | **PARTIAL** | Estado en SQLite (`data/`, git-ignored) sobrevive reinicios; reset documentado (`rm data/*.db`). Sin procedimiento probado automatizado. |
| RNF-11 | Perfil operativo en < 10 min | **PARTIAL** | Perfil local: segundos. K8s: no medido. |

---

## Known limitations (perfil local)

1. El speed layer es un **CLI por polling**, no un demonio residente: no hay latencia streaming continua medible (RF-09 parcial).
2. El lakehouse local es **SQLite mononodo**: el contrato (append idempotente, versionado, evolución) es el mismo que en Iceberg, pero no hay almacenamiento columnar ni multi-motor.
3. El motor de calidad evalúa **suites en formato Great Expectations**, pero **no es la librería GE**.
4. **No hay pipeline CI**: las validaciones bloquean en ejecución (`exit 1`), no en merge.
5. Telemetría (Prometheus/Grafana), catálogo UI (Marquez) y admisión (Kyverno) son **perfil K8s**, no verificados aquí.
6. `flink_clickstream_stream.py` agrega ventanas del topic clickstream; métricas funnel/sesión están fuera de alcance (SRS §1.2).
7. La fusión del serving es por clave, no por recencia: una fila stream cuya `(order_date, status)` ya existe en batch queda ensombrecida aunque sea más reciente (regla batch-wins, RF-11). El stream aporta claves ausentes del batch (datos posteriores a la última recomputación con fecha/estado nuevos).
