# Log de decisiones técnicas

Documento vivo que registra las decisiones de diseño e implementación del proyecto **Cloud Provider Analytics**. Se actualiza a medida que avanza el MVP del segundo parcial.

**Última actualización:** 2026-06-13

---

## 1. Patrón arquitectónico: Lambda vs Kappa

| Decisión | **Lambda Architecture** |
|---|---|
| **Estado** | Acordado (entrega 1) · en implementación |
| **Motivo** | El dominio mezcla datos naturalmente **batch** (maestros CSV, facturación mensual) con datos **near real-time** (eventos JSONL). Forzar todo como stream (Kappa) agrega complejidad sin beneficio para CSV periódicos. |
| **Camino batch** | PySpark batch: `customers_orgs`, `users`, `billing_monthly` (+ extensión futura a otros maestros) |
| **Camino streaming** | Spark Structured Streaming: `usage_events_stream/*.jsonl` |
| **Convergencia** | Ambos caminos comparten zonas `bronze/`, `silver/`, `gold/` y alimentan Cassandra/AstraDB |
| **Kappa descartada porque** | Requeriría simular maestros como stream, dificulta versionado batch y el reprocesamiento histórico de CSV |

---

## 2. Estructura del datalake y rutas

| Decisión | Detalle |
|---|---|
| **Layout de zonas** | `landing/` → `bronze/` → `silver/` → `gold/` + `quarantine/` + `checkpoints/` |
| **Landing inmutable** | Sin transformaciones; fuente de verdad raw del dataset de la consigna |
| **`DATA_ROOT`** | Por defecto `./datalake` dentro del repo; override con env `DATA_ROOT` (Colab: `/content/datalake`) |
| **`source_file` relativo** | Se guarda ruta lógica bajo el datalake, no path absoluto del SO. Ej.: `landing/customers_orgs.csv`, `landing/usage_events_stream/events_part_0001.jsonl` |
| **Motivo** | Portabilidad entre local/Colab y trazabilidad alineada al layout del lake |

---

## 3. Bronze batch (implementado)

| Tema | Decisión | Motivo |
|---|---|---|
| **Datasets MVP** | `customers_orgs`, `users`, `billing_monthly` | Mínimo exigido por consigna parcial 2 |
| **Formato salida** | Parquet (compresión Snappy por default Spark) | Requisito consigna + eficiente para capas superiores |
| **Particionado** | `partitionBy("ingest_date")` | Separa corridas de ingesta; alineado al diseño de entrega 1 |
| **Modo escritura** | `overwrite` por dataset | Idempotencia simple en maestros pequeños: re-ejecutar deja un snapshot consistente |
| **Schema** | `StructType` explícito por CSV; sin `inferSchema` | Control de tipos y reproducibilidad |
| **Tipificación** | CSV leído como string donde hay ambigüedad; cast explícito a `boolean`, `date`, `double` | El dataset trae booleanos como `"True"/"False"` y campos nullable |
| **Columnas técnicas** | `ingest_ts`, `ingest_date`, `source_file` | Auditoría y trazabilidad |
| **Dedupe** | Por clave natural; conservar fila con `ingest_ts` más reciente | `org_id`, `user_id`, `invoice_id` |
| **Calidad en Bronze** | Ninguna regla activa de negocio | Bronze solo tipifica y audita; quarantine va en Silver |
| **Evidencias** | Conteos `raw_count`, `deduped_count`, `written_count` | Cumple checklist de idempotencia del parcial |

**Conteos validados (2026-06-13):** 80 + 800 + 240 filas; unicidad OK en las tres claves.

---

## 4. Bronze streaming (implementado)

| Tema | Decisión | Motivo |
|---|---|---|
| **Fuente** | `landing/usage_events_stream/*.jsonl` | Formato apto para Structured Streaming |
| **Schema** | Unificado v1/v2: `carbon_kg` y `genai_tokens` como `nullable` | Schema evolution ~2025-07-18 sin romper lectura |
| **Campo `value`** | Leído como string en schema; normalizado a `double` en transform | El dataset mezcla `124.0`, `"109.0"` y `null` |
| **Watermark técnico** | **Producción:** `10 minutes` · **Replay landing estático:** `60 days` (env `STREAMING_WATERMARK`) | Con `10 minutes` + `availableNow` sobre ~60 días de histórico, Spark descartaba ~67% de eventos al avanzar el max `event_ts`. El replay del landing necesita ventana ≥ span del dataset |
| **Late data (negocio)** | `is_late_arrival` si `event_latency_sec > 600` (10 min) | Umbral de negocio independiente del watermark técnico; auditable en Bronze |
| **Dedupe** | `dropDuplicates(["event_id"])` post-watermark | Requisito consigna; estado acotado por watermark |
| **Checkpoint** | `checkpoints/usage_events_bronze/` | Exactly-once lógico y reanudación |
| **Trigger desarrollo** | `availableNow=True` + `maxFilesPerTrigger=20` | Procesa todo el landing en corrida reproducible |
| **Particionado salida** | `partitionBy("ingest_date")` | Consistente con batch Bronze |
| **Modo escritura** | `append` (streaming) | Semántica correcta para micro-batches |

**Conteos validados (2026-06-13):** 43.200 eventos; 43.200 `event_id` únicos; v1=10.800 / v2=32.400; re-ejecución con checkpoint sin duplicar.

---

## 5. Silver (implementado)

| Tema | Decisión | Motivo |
|---|---|---|
| **Alcance MVP** | Eventos + maestro `customers_orgs` | Mínimo consigna parcial 2 |
| **Motor** | Job batch PySpark sobre Parquet Bronze | Bronze streaming ya materializado; Silver corre como batch idempotente |
| **Orden de ejecución** | Primero `customers_orgs`, luego `usage_events` | El join de eventos depende del maestro Silver |
| **Join** | `usage_events` LEFT JOIN `customers_orgs` por `org_id` | Enriquecimiento con `org_name`, `org_industry`, `org_plan_tier`, etc. |
| **Features** | `usage_date`, `daily_cost_usd`, `requests`, `genai_tokens`, `carbon_kg` | `daily_cost_usd = cost_usd_increment` a nivel evento; Gold agrega por día |
| **Schema v1/v2** | `coalesce(genai_tokens, 0)`, `coalesce(carbon_kg, 0)` | No romper métricas históricas sin campos v2 |
| **Regla 1** | `event_id` nulo o duplicado → quarantine | Integridad del hecho; dedupe conserva el más reciente por `event_ts` |
| **Regla 2** | `cost_usd_increment < -0.01` → flag `is_cost_anomaly` (no quarantine) | Tolerancia a pequeños ajustes/créditos; auditable sin perder volumen |
| **Regla 3** | `value` presente y `unit` nulo → quarantine | Conformance de métricas |
| **Orphans** | `org_id` sin match en maestro → quarantine | Preparado; en el dataset actual no hay huérfanos |
| **Quarantine** | `quarantine/silver/usage_events/` con `error_reason`, `quarantine_ts` | No bloquea el pipeline principal |
| **Particionado Silver** | `usage_date` (eventos), `ingest_date` (maestro) | Consultas por rango temporal en Gold |
| **Modo escritura** | `overwrite` | Idempotencia en re-ejecución |

**Conteos validados (2026-06-13):** Bronze 43.200 → Silver 41.162 + Quarantine 2.038; 206 anomalías de costo flaggeadas; balance OK.

---

## 6. Gold (implementado)

| Tema | Decisión | Motivo |
|---|---|---|
| **Mart MVP** | `org_daily_usage_by_service` | Obligatorio en consigna parcial 2 |
| **Grano** | `(org_id, usage_date, service)` | Consulta FinOps diaria por organización y servicio |
| **Fuente** | `silver/usage_events` | Solo eventos válidos (post-quarantine) |
| **Métricas** | `total_daily_cost_usd`, `total_requests`, `total_genai_tokens`, `total_carbon_kg`, `event_count` | Agregación de features Silver |
| **Auditoría anomalías** | `anomaly_event_count`, `has_cost_anomaly` | Trazabilidad de flags de costo sin excluir datos |
| **Dimensiones** | `org_name`, `org_industry`, `org_plan_tier` (first por grupo) | Contexto de negocio en el mart |
| **Particionado** | `partitionBy("usage_date")` | Lecturas por rango temporal hacia Cassandra/BI |
| **Modo escritura** | `overwrite` | Idempotencia en re-ejecución |
| **Validación** | Grano único + balance de costos Silver vs Gold | Evidencia de integridad referencial entre capas |

**Conteos validados (2026-06-13):** 41.162 eventos Silver → 12.114 filas Gold; costo total USD 140.327,94 (balance OK).

---

## 7. Cassandra / AstraDB (implementado)

| Tema | Decisión | Motivo |
|---|---|---|
| **Keyspace** | `cloud_analytics` | Convención del enunciado |
| **Tabla MVP** | `org_daily_usage_by_service` | Alineada al mart Gold |
| **Partition key** | `org_id` | Consultas por organización sin `ALLOW FILTERING` |
| **Clustering** | `usage_date DESC`, `service ASC` | Rango temporal + desglose por servicio |
| **PK** | `PRIMARY KEY ((org_id), usage_date, service)` | Modelo query-first |
| **Keyspace en Astra** | Crear `cloud_analytics` en consola (no vía CQL) | Astra bloquea `CREATE KEYSPACE` por driver |
| **Carga** | `cassandra-driver` + prepared `INSERT` desde Gold Parquet | Portable en Colab sin Spark Connector JAR |
| **Idempotencia** | Upsert implícito por PK (re-INSERT sobrescribe) | Re-cargar Gold no duplica |
| **DDL** | `cql/00_create_tables.cql` | Tabla query-first; keyspace solo en consola Astra |
| **Consulta #1** | Costos/requests por org+servicio en rango de fechas | CQL directo sobre la tabla |
| **Consulta #2** | Top-N servicios por costo acumulado | Filas por PK + agregación por `service` en driver (patrón query-first) |
| **Credenciales** | `ASTRA_DB_APPLICATION_TOKEN` + `ASTRA_DB_SECURE_BUNDLE_PATH` (obligatorios) | Solo AstraDB vía `cassandra-driver` |
| **TTL** | Sin TTL | Agregados FinOps deben persistir |

**Scripts:** `cql/00_create_tables.cql`, `cql/01_daily_costs_and_requests.cql`, `cql/02_top_services_by_cost.cql` · **Job:** `src/jobs/serving_cassandra.py` · **Notebook:** `pipeline_completo.ipynb`

---

## 8. Umbrales y parámetros de referencia

| Parámetro | Valor | Ámbito | Configuración |
|---|---|---|---|
| Watermark producción | `10 minutes` | Streaming Silver/Gold en vivo | `WATERMARK_DELAY_PRODUCTION` |
| Watermark replay estático | `60 days` | Bronze streaming sobre landing histórico | `STREAMING_WATERMARK` (env) |
| Late arrival (negocio) | `600` segundos | Flag `is_late_arrival` en Bronze | `LATE_DATA_THRESHOLD_SEC` |
| Tolerancia costo negativo | `>= -0.01` USD (flag `is_cost_anomaly`) | Regla calidad Silver | `COST_ANOMALY_THRESHOLD` |
| Fecha futura inválida | `event_ts > now() + 5 min` | Quarantine Silver | Pendiente (entrega 1) |
| Retraso máximo catch-up | `30 minutes` | Quarantine si supera umbral | Pendiente (entrega 1) |
| Trigger streaming dev | `availableNow` | Notebooks / corridas locales | Código job |
| `maxFilesPerTrigger` | `20` | Bronze streaming | Código job |

---

## 9. Idempotencia y reprocesamiento

| Capa | Estrategia |
|---|---|
| **Bronze batch** | `overwrite` + dedupe por clave natural antes de escribir |
| **Bronze streaming** | Checkpoint + dedupe `event_id`; re-ejecución sin `reset_state` no duplica |
| **Reset desarrollo** | `reset_streaming_state()` borra `bronze/usage_events/` y checkpoint para corrida limpia |
| **Silver/Gold** | Silver: `overwrite` por dataset; balance `bronze = silver + quarantine` |
| **Cassandra** | Upsert por PK `(org_id, usage_date, service)` vía prepared INSERT |

---

## 10. Entorno de ejecución

| Decisión | Detalle |
|---|---|
| **Runtime** | Google Colab + ejecución local con `.venv` |
| **Módulos** | Lógica en `src/`; orquestación y evidencias en `notebooks/pipeline_completo.ipynb` |
| **Spark** | `local[*]` en desarrollo; PySpark ≥ 3.5 |
| **Dependencias** | `requirements.txt` en raíz del repo |

---

## Historial de cambios

| Fecha | Cambio |
|---|---|
| 2026-06-13 | Creación del log. Bronze batch y streaming implementados. |
| 2026-06-13 | `source_file` pasa de path absoluto a ruta relativa `landing/...`. |
| 2026-06-13 | Watermark de replay documentado (`60 days`) vs producción (`10 minutes`). |
| 2026-06-13 | README Quickstart end-to-end agregado en raíz del repo. |

---

## Próximas entradas esperadas

- [ ] Capturas de consultas AstraDB en notebook (requiere credenciales).
