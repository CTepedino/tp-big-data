# Log de decisiones técnicas

## 1. Patrón arquitectónico: Lambda vs Kappa

| Decisión | **Lambda Architecture** |
|---|---|
| **Motivo** | El dominio mezcla datos naturalmente **batch** (maestros CSV, facturación mensual) con datos **near real-time** (eventos JSONL). Forzar todo como stream (Kappa) agrega complejidad sin beneficio para CSV periódicos. |
| **Camino batch** | PySpark batch: `customers_orgs`, `users`, `billing_monthly`, `resources`, `support_tickets`, `marketing_touches`, `nps_surveys` |
| **Camino streaming** | Spark Structured Streaming: `usage_events_stream/*.jsonl` |
| **Convergencia** | Ambos caminos comparten zonas `bronze/`, `silver/`, `gold/` y alimentan Cassandra/AstraDB |
| **Kappa descartada porque** | Requeriría simular maestros como stream, dificulta versionado batch y el reprocesamiento histórico de CSV |

---

## 2. Estructura del datalake y rutas

| Decisión | Detalle |
|---|---|
| **Layout de zonas** | `landing/` → `bronze/` → `silver/` → `gold/` + `quarantine/` + `checkpoints/` |
| **Landing inmutable** | Sin transformaciones; fuente de verdad raw del dataset de la consigna |
| **`DATA_ROOT`** | Por defecto `datalake/` bajo `REPO_ROOT` (`src/config.py` vía `__file__`); override con env `DATA_ROOT` (resuelto con `abspath`) |
| **`source_file` relativo** | Se guarda ruta lógica bajo el datalake, no path absoluto del SO. Ej.: `landing/customers_orgs.csv`, `landing/usage_events_stream/events_part_0001.jsonl` |
| **Diagrama** | Quarantine (rama desde Silver) y checkpoints (streaming bronze + serving load) dentro del recuadro DataLake — ver `Diagram.png` |
| **Motivo** | Portabilidad local con `.venv` y trazabilidad alineada al layout del lake |

---

## 3. Bronze batch (implementado)

| Tema | Decisión | Motivo |
|---|---|---|
| **Datasets** | `customers_orgs`, `users`, `billing_monthly`, `resources`, `support_tickets`, `marketing_touches`, `nps_surveys` | Bronze batch completo (TP final) |
| **Formato salida** | Parquet (compresión Snappy por default Spark) | Requisito consigna + eficiente para capas superiores |
| **Particionado** | `partitionBy("ingest_date")` | Separa corridas de ingesta; alineado al diseño de entrega 1 |
| **Modo escritura** | `overwrite` por dataset | Idempotencia simple en maestros pequeños: re-ejecutar deja un snapshot consistente |
| **Schema** | `StructType` explícito por CSV; sin `inferSchema` | Control de tipos y reproducibilidad |
| **Tipificación** | CSV leído como string donde hay ambigüedad; cast explícito a `boolean`, `date`, `double` | El dataset trae booleanos como `"True"/"False"` y campos nullable |
| **Columnas técnicas** | `ingest_ts`, `ingest_date`, `source_file` | Auditoría y trazabilidad |
| **Dedupe** | Por clave natural; conservar fila con `ingest_ts` más reciente | `org_id`, `user_id`, `invoice_id`, `resource_id`, `ticket_id`, `touch_id`, `(org_id, survey_date)` |
| **Calidad en Bronze** | Ninguna regla activa de negocio | Bronze solo tipifica y audita; quarantine va en Silver |
| **Evidencias** | Conteos `raw_count`, `deduped_count`, `written_count` | Cumple checklist de idempotencia del parcial |

**Conteos validados (corrida limpia 2026-06-13):** maestros 80 + 800 + 240 + 400 + 1000 + 1500 + 92; unicidad OK en todas las claves. Ver README para conteos Gold/Silver streaming.

---

## 4. Bronze streaming (implementado)

| Tema | Decisión | Motivo |
|---|---|---|
| **Fuente** | `landing/usage_events_stream/*.jsonl` | Formato apto para Structured Streaming |
| **Schema** | Unificado v1/v2: `carbon_kg` y `genai_tokens` como `nullable` | Schema evolution ~2025-07-18 sin romper lectura |
| **Campo `value`** | Leído como string en schema; normalizado a `double` en transform | El dataset mezcla `124.0`, `"109.0"` y `null` |
| **Watermark técnico** | **Producción:** `10 minutes` · **Replay landing estático:** `60 days` (env `STREAMING_WATERMARK`) | Con `10 minutes` + `availableNow` sobre ~60 días de histórico, Spark descartaba ~67% de eventos al avanzar el max `event_ts`. El replay del landing necesita ventana ≥ span del dataset |
| **Alcance watermark** | Solo **Bronze streaming** (`withWatermark` en `transform_usage_events_bronze`); Silver y Gold son batch | No hay Structured Streaming en capas superiores |
| **Late data (negocio)** | Modelo 3 tiers: `event_latency_sec` en Bronze; `is_late_arrival` si > 600 s (10 min) | Umbral de negocio independiente del watermark técnico |
| **Late data (replay)** | `ingest_ts = event_ts` cuando watermark ≠ producción | Evita falsos late al replay del landing estático con `current_timestamp()` |
| **Late data (producción)** | `ingest_ts = current_timestamp()` | Latencia real ingest − evento |
| **Dedupe** | `dropDuplicates(["event_id"])` post-watermark | Requisito consigna; estado acotado por watermark |
| **Normalización temprana** | `service`, `region`, `metric`, `unit`: trim + lower + aliases (`src/schemas/normalization.py`) | Conformance antes de materializar Bronze; alineado a Silver |
| **Checkpoint streaming** | `checkpoints/usage_events_bronze/` | Exactly-once lógico y reanudación del job Bronze streaming |
| **Trigger desarrollo** | `availableNow=True` + `maxFilesPerTrigger=20` | Procesa todo el landing en corrida reproducible |
| **Particionado stream** | `partitionBy("ingest_date")` en escritura append | Consistente con convención Bronze batch |
| **Layout final eventos** | Tras stream: **reparquet** a `partitionBy(usage_date, service)` | Lecturas por rango de negocio en Silver/Gold; ver §8 |
| **Reparquet idempotente** | Si rerun sin `reset_state` y `streaming_input_rows = 0` con layout ya reparquetado → **skip reparquet** | Evita relayout innecesario y particiones `ingest_date` vacías que rompen `read.parquet` |
| **Lectura Bronze eventos** | `read_bronze_usage_events_parquet()` limpia particiones vacías + `mergeSchema` | Usado en streaming validate, Silver e idempotencia notebook |
| **Modo escritura** | `append` (streaming) | Semántica correcta para micro-batches |

**Conteos validados (2026-06-13):** 43.200 eventos; 43.200 `event_id` únicos; v1=10.800 / v2=32.400; re-ejecución con checkpoint sin duplicar.

---

## 5. Silver (implementado — TP final)

| Tema | Decisión | Motivo |
|---|---|---|
| **Alcance** | 7 maestros + `usage_events` | Bronze batch completo + streaming materializado |
| **Motor** | Job batch PySpark sobre Parquet Bronze | Bronze streaming ya materializado; Silver corre como batch idempotente (Lambda) |
| **Orden de ejecución** | Maestros (`customers_orgs` primero) → `usage_events` | Joins de eventos dependen de maestros Silver |
| **Maestros** | Normalización conformance + `silver_ts`; quarantine solo PK nula u `org_id` huérfano | Catálogo inválido (`service`/`region`) → quarantine solo en `usage_events` (§12) |
| **Join eventos** | `customers_orgs`, `resources`, stats de `users` por `org_id` | Enriquecimiento org + recurso + adopción de usuarios |
| **Canonical service/region** | Tras join: `coalesce(resource_*, evento)` con catálogo normalizado | Maestro `resources` gana si el evento trae ruido |
| **Features evento** | `usage_date`, `cost_usd_increment`, `requests`, `cpu_hours`, `storage_gb_hours`, `genai_tokens`, `carbon_kg` | Grano evento para anomalías, late data y quarantine |
| **Features agregadas** | `silver/org_service_daily` grano `(org_id, usage_date, service)` con `daily_cost_usd` | Consigna completa: `daily_cost_usd` por org y servicio |
| **Schema v1/v2** | `coalesce(genai_tokens, 0)`, `coalesce(carbon_kg, 0)` | No romper métricas históricas sin campos v2 |
| **Regla 1** | `event_id` nulo o duplicado → quarantine | Integridad del hecho; dedupe conserva el más reciente por `event_ts` |
| **Regla 2** | `value` presente y `unit` nulo → quarantine | Conformance de métricas |
| **Regla 3** | `org_id` o `resource_id` sin match en maestro → quarantine | Integridad referencial |
| **Regla 4** | `event_latency_sec > 1800` (30 min) → quarantine | Late extremo aislado; late suave (10–30 min) sigue en Silver con `is_late_arrival` |
| **Regla 5** | `event_ts > now() + 5 min` → quarantine | Fechas futuras inválidas (`FUTURE_EVENT_TOLERANCE_SEC`) |
| **Regla 6** | `cost_usd_increment < -0.01` → quarantine | Consigna: dominio `[-0.01, +∞)`; no mezclar con flag de anomalía |
| **Regla 7** | `event_ts` nulo o no parseable → quarantine | Fechas inválidas tras `to_timestamp` |
| **Regla 8** | `service` / `region` fuera de catálogo cloud → quarantine | Solo si valor no nulo tras normalizar; defensivo ante landing ruidoso |
| **Anomalías costo** | Z-score (`|z|>3`), MAD (`|z_mad|>3.5`), percentiles (P1/P99), **p99×X** (`X=2.0`) | `is_cost_anomaly` = OR de los 4 flags; **no** quarantine |
| **Quarantine** | `quarantine/silver/<dataset>/` con `error_reason`, `quarantine_ts` | No bloquea el pipeline principal |
| **Particionado Silver** | `usage_date` (eventos), `ingest_date` (maestros) | Consultas por rango temporal en Gold |
| **Modo escritura** | `overwrite` | Idempotencia en re-ejecución |

**Conteos validados (2026-06-13):** Bronze 43.200 → Silver **40.956** válidos + Quarantine **2.249** (union multi-regla; ~2.244 `event_id` únicos en quarantine); maestros sin pérdida. Desglose quarantine eventos: ~211 costo < −0.01.

---

## 6. Gold (implementado — TP final)

| Tema | Decisión | Motivo |
|---|---|---|
| **Marts servidos (5)** | Tablas Astra alineadas a consultas #1–#5 | Query-first; obligatorio para demo CQL |
| **Total marts Gold** | **8** (5 servidos + 3 solo Parquet) | Ver marts Gold-only abajo |
| **Marts Gold-only (3)** | `cost_anomaly_mart`, `nps_by_org_date`, `marketing_touches_by_org_channel` | Analítica en Parquet; consigna §4 Gold / maestros CRM sin consulta CQL mínima |
| **org_daily_usage_by_service** | Grano `(org_id, usage_date, service)` | Consulta #1 FinOps diaria · **servido** |
| **org_top_services_by_cost** | Grano `(org_id, period_end, rank, service)` · ventana rolling 14d hasta `max(usage_date)` | Consulta #2 · **servido** |
| **revenue_by_org_month** | Grano `(org_id, billing_month)` con `invoice_count` y montos USD | Consulta #4 · **servido** |
| **cost_anomaly_mart** | Grano `(org_id, usage_date, service)` + `anomaly_score` | FinOps interno; **no servido** (no es consulta #1–#5) |
| **tickets_by_org_date** | Grano `(org_id, ticket_date, severity)`; `ticket_date` = `created_at` del ticket | Consulta #3 · **servido** |
| **genai_tokens_by_org_date** | Grano `(org_id, usage_date)` | Consulta #5 · **servido** |
| **Consulta #5 — costo** | `estimated_cost_usd` = `sum(cost_usd_increment)` en eventos con `service=genai` o `genai_tokens>0` | No es `tokens × tarifa`; usa costo incremental del evento en el landing |
| **nps_by_org_date** | Grano `(org_id, survey_date)` · `avg_nps_score`, `survey_count` | CRM / satisfacción; **no servido** |
| **marketing_touches_by_org_channel** | Grano `(org_id, touch_date, channel)` · clicks/conversiones | Producto/marketing; **no servido** |
| **Fuente principal** | `silver/org_service_daily` (FinOps diario), `silver/usage_events` (anomalías/GenAI) + maestros | Grano correcto por capa |
| **Métricas usage** | costo, requests, cpu_hours, storage_gb_hours, genai, carbon | Features Silver agregadas |
| **Revenue FX** | `subtotal` / `credits` / `taxes` × `exchange_rate_to_usd`; `revenue_usd = subtotal_usd − credits_usd + taxes_usd` | Normalización multi-moneda en `build_revenue_by_org_month` |
| **Particionado** | `usage_date`, `billing_month`, `ticket_date`, `period_end` | Lecturas por rango temporal |
| **Modo escritura** | `overwrite` | Idempotencia en re-ejecución |
| **Validación** | Grano único + balances costo/tickets/tokens | Integridad entre capas |

**Conteos validados (corrida limpia 2026-06-13):** FinOps **12.108**; top services **258**; revenue **240**; tickets **984**; genai **1.235**; Gold-only — anomaly **12.108** (**6.351** con flag); nps **92**; marketing **1.477**.

---

## 7. Cassandra / AstraDB (implementado — TP final)

| Tema | Decisión | Motivo |
|---|---|---|
| **Keyspace** | `cloud_analytics` | Convención del enunciado |
| **Tablas (5)** | Una por consulta mínima del enunciado | Modelo query-first; marts Gold adicionales quedan en Parquet |
| **org_daily_usage_by_service** | PK `((org_id), usage_date, service)` | Consulta #1 |
| **org_top_services_by_cost** | PK `((org_id, period_end), rank, service)` | Consulta #2 en CQL puro (mart Gold + carga Serving) |
| **tickets_by_org_date** | PK `((org_id, severity), ticket_date)` | Consulta #3 tickets críticos + SLA por rango |
| **Consulta #3 — tickets críticos** | Filtro CQL/demo `severity = 'high'`; severidades del landing: `low`, `medium`, `high` | En el enunciado “críticos” = severidad máxima (`high`); evolución SLA por día en ventana rolling 30d |
| **Consulta #3 — días sin tickets** | Solo días con actividad para la severidad filtrada (sparse); sin fila si no hay tickets ese día | Modelo Cassandra pre-agregado; días con tickets y sin breach muestran `sla_breach_rate = 0` |
| **revenue_by_org_month** | PK `((org_id), billing_month)` | Consulta #4 revenue USD agregado por mes |
| **genai_tokens_by_org_date** | PK `((org_id), usage_date)` | Consulta #5 tokens + `estimated_cost_usd` |
| **Keyspace en Astra** | Crear `cloud_analytics` en consola (no vía CQL) | Astra bloquea `CREATE KEYSPACE` por driver |
| **Carga** | Structured Streaming `foreachBatch` sobre Gold Parquet → prepared INSERT (driver dentro de cada micro-batch) | Cumple consigna §5 (`foreachBatch` + driver Python); consultas demo con `cassandra-driver` |
| **Checkpoint serving** | `checkpoints/serving_foreach_<table>/` por tabla cargada | Idempotencia de la carga foreachBatch |
| **Top-N (#2)** | Mart Gold `org_top_services_by_cost`: ventana rolling 14 días anclada a `max(usage_date)`; Serving DELETE `(org_id, period_end)` + INSERT | Evita agregación en app; ranks obsoletos no persisten |
| **Tickets (#3)** | Ventana demo rolling 30 días hasta `max(ticket_date)` por org + `severity=high` (`TICKETS_CRITICAL_LOOKBACK_DAYS`) | Alineado a enunciado “últimos 30 días” |
| **Ventanas demo CQL** | **#2 y #3:** rolling desde Gold (`demo.py`) · **#1, #4 y #5:** rangos fijos en `DemoQueryParams` (ej. `2025-07-01`–`2025-08-31`) | Alineados al span del dataset demo; notebook §7 |
| **Revenue (#4)** | Carga directa desde Gold `(org_id, billing_month)` | Mismo grano que Cassandra; sin re-agregar en Serving |
| **Idempotencia carga** | Upsert implícito por PK de cada tabla; top-N: DELETE partición antes de INSERT | Re-cargar Gold no duplica filas por PK |
| **DDL** | `cql/00_create_tables.cql` | 5 tablas query-first |
| **Credenciales** | `ASTRA_DB_APPLICATION_TOKEN` + `ASTRA_DB_SECURE_BUNDLE_PATH` | Solo AstraDB vía `cassandra-driver` |
| **CLI** | `--skip-load` (solo DDL) · `--demo-queries` (carga + consultas #1–#5) | Ver `src/jobs/serving_cassandra.py` |
| **TTL** | Sin TTL | Agregados analíticos deben persistir |

**Módulo Cassandra:** `src/cassandra/` — `schema.py`, `inserts.py`, `selects.py`, `demo.py`, `client.py`, `foreach_batch_loader.py` (opcional: `spark_connector.py`).

**Scripts:** `cql/00`–`05` · **Carga:** `src/jobs/serving_cassandra.py` · **Consultas demo:** `src/cassandra/selects.py` + `demo.py` · **Notebook:** `pipeline.ipynb` (raíz) §5 carga, §7 CQL

---

## 8. Performance Spark (consigna §7)

| Tema | Decisión | Motivo |
|---|---|---|
| **Módulo central** | `src/spark/performance.py` | `configure_spark_performance()` + `write_partitioned_parquet()` reutilizable en jobs |
| **Shuffle** | `spark.sql.shuffle.partitions` = `SPARK_SHUFFLE_PARTITIONS` (default 16) + AQE + `coalescePartitions` | Escala joins Silver sin hardcodear particiones del cluster |
| **Maestros (coalesce)** | Bronze batch y Silver maestros: `coalesce(SPARK_TARGET_FILES_MASTER)` antes de `partitionBy(ingest_date)` | CSV pequeños → pocos archivos Parquet; evita directorios con cientos de shards |
| **Eventos (repartition)** | Silver `usage_events`: `repartition(..., "usage_date")` antes de escribir; joins con `repartition(..., "org_id")` | Alinea shuffle a grano analítico y reduce skew en joins con dimensiones |
| **Broadcast** | `customers`, `resources`, `org_user_stats` en Silver | Tablas maestras << eventos; evita shuffle join innecesario |
| **Reparquet Bronze streaming** | Post-stream: `repartition(usage_date, service)` + `partitionBy(usage_date, service)` | El stream escribe por `ingest_date` (append); re-layout por fecha de negocio + servicio |
| **Gold** | `repartition(SPARK_TARGET_FILES_GOLD, <partition_col>)` por mart (`usage_date`, `billing_month`, `ticket_date`) | Particiones equilibradas alineadas al grano de consulta |
| **Evidencia** | `reparquet` stats en salida de `run_streaming_bronze` (`parquet_files_before/after`, `skipped` si idempotente) | Trazabilidad de compactación |

**Variables:** `SPARK_SHUFFLE_PARTITIONS`, `SPARK_TARGET_FILES_MASTER`, `SPARK_TARGET_FILES_EVENTS`, `SPARK_TARGET_FILES_GOLD` (ver README).

---

## 9. Umbrales y parámetros de referencia

| Parámetro | Valor | Ámbito | Fuente |
|---|---|---|---|
| Watermark producción | `10 minutes` | Bronze streaming (event-time) | `WATERMARK_DELAY_PRODUCTION` en `schemas/bronze_streaming.py` |
| Watermark replay estático | `60 days` | Bronze streaming sobre landing histórico | env `STREAMING_WATERMARK` → `WATERMARK_DELAY` |
| Late arrival (negocio) | `600` segundos | Flag `is_late_arrival` en Bronze/Silver válido | `LATE_DATA_THRESHOLD_SEC` en `schemas/bronze_streaming.py` |
| Retraso máximo catch-up | `1800` segundos (30 min) | Quarantine Silver si supera umbral | `LATE_CATCHUP_MAX_SEC` en `schemas/bronze_streaming.py` |
| Fecha futura inválida | `300` segundos (5 min) | Quarantine Silver si `event_ts` supera tolerancia | `FUTURE_EVENT_TOLERANCE_SEC` en `schemas/silver.py` |
| Costo mínimo válido | `-0.01` USD | Quarantine Silver si `cost_usd_increment` menor | `COST_USD_INCREMENT_MIN` en `schemas/silver.py` |
| Anomalía p99×X | `X = 2.0` | Flag `is_cost_anomaly_p99_x` si costo > p99×X por org/servicio | `P99_ANOMALY_MULTIPLIER` en `schemas/silver.py` |
| Z-score costo | `3.0` | Flag `is_cost_anomaly_zscore` | `ZSCORE_THRESHOLD` en `schemas/silver.py` |
| MAD modificado | `3.5` | Flag `is_cost_anomaly_mad` | `MAD_THRESHOLD` en `schemas/silver.py` |
| Percentiles costo | P1 / P99 | Flag `is_cost_anomaly_percentile` | `PERCENTILE_LOW` / `PERCENTILE_HIGH` en `schemas/silver.py` |
| Trigger streaming dev | `availableNow` | Notebooks / corridas locales | Código job |
| `maxFilesPerTrigger` | `20` | Bronze streaming | Código job |
| Top servicios (#2) | 14 días | Ventana rolling FinOps | `TOP_SERVICES_LOOKBACK_DAYS` en `src/cassandra/schema.py` |
| Tickets críticos (#3) | 30 días | Ventana rolling tickets | `TICKETS_CRITICAL_LOOKBACK_DAYS` en `src/cassandra/schema.py` |

---

## 10. Idempotencia y reprocesamiento

| Capa | Estrategia |
|---|---|
| **Bronze batch** | `overwrite` + dedupe por clave natural antes de escribir |
| **Bronze streaming** | Checkpoint `usage_events_bronze` + dedupe `event_id`; rerun sin `reset_state` no duplica eventos |
| **Bronze streaming reparquet** | Skip si no hay filas nuevas y layout `usage_date/service` ya aplicado; limpieza de particiones `ingest_date` vacías antes de leer |
| **Reset desarrollo** | `reset_streaming_state()` borra `bronze/usage_events/` y checkpoint streaming para corrida limpia |
| **Silver/Gold** | `overwrite` por dataset; balance `bronze ≈ silver válido + quarantine` (union multi-regla puede inflar `quarantine_count` en stats) |
| **Cassandra** | Upsert por PK de cada tabla vía prepared INSERT; `org_top_services_by_cost`: DELETE `(org_id, period_end)` + INSERT; checkpoints `serving_foreach_*` en foreachBatch |
| **Notebook §6** | Re-run batch → streaming (sin reset) → Silver → Gold; validación lake + sample org en Cassandra |

---

## 11. Entorno de ejecución

| Decisión | Detalle |
|---|---|
| **Runtime** | Ejecución local con `.venv` |
| **Módulos** | Lógica en `src/`; orquestación en `pipeline.ipynb` (raíz del repo) |
| **Spark** | `local[*]` en desarrollo; PySpark ≥ 3.5; `src/spark/java.py` (`ensure_java_home`) para notebook sin `JAVA_HOME` |
| **Dependencias** | `requirements.txt` en raíz del repo |
| **Config** | `.env` vía `src.config`: Astra (`ASTRA_DB_*`), `DATA_ROOT`, `CASSANDRA_KEYSPACE`; watermark replay: env `STREAMING_WATERMARK`. Umbrales late/costo/anomalía: constantes en `schemas/` (§9) |

---

## 12. Normalización y conformance (§3 consigna)

| Tema | Decisión | Motivo |
|---|---|---|
| **Módulo** | `src/schemas/normalization.py` | Reglas reutilizables Bronze + Silver |
| **Strings** | `trim` + `lower`; `""` → `null` | Categóricos comparables en joins y agregaciones |
| **Regiones** | Además `_` → `-` (ej. `us_east` → `us-east`) | Alias geográfico sin tabla externa |
| **Servicios** | Mapa de aliases (`computing`→`compute`, `ai`→`genai`, …) | Tolerar sinónimos del landing |
| **Catálogo** | 6 servicios + 7 regiones del challenge | Quarantine en `usage_events` si valor no nulo y fuera de lista |
| **Maestros Silver** | Normalización en columnas de texto (`hq_region`, `service`, `channel`, `severity`, …) | Conformance; sin quarantine por catálogo en maestros |
| **Eventos** | Normalización en Bronze streaming + Silver post-join | Doble capa: temprana en ingesta, canonical con maestro en Silver |
| **Fechas evento** | `event_ts` vía `to_timestamp`; `usage_date = to_date(event_ts)` | Silver rechaza timestamps no parseables |
| **Agregación diaria** | En batch Silver/Gold, no ventana tumbling en stream | Lambda: near-RT en ingesta; métricas diarias en convergencia batch |

**Nota streaming “ventanas”:** watermark = ventana de event-time para late data en Bronze; agregaciones por intervalo (10 min) no implementadas en stream — grano diario en `org_service_daily` / Gold.
