# Diccionario de datos clave

Referencia de columnas principales por capa. Rutas bajo `datalake/` salvo keyspace Astra `cloud_analytics`.

---

## Convenciones técnicas (todas las capas)

| Columna | Tipo | Descripción |
|---|---|---|
| `ingest_ts` | timestamp | Momento de ingesta al lake |
| `ingest_date` | date | Partición de corrida (`ingest_date=YYYY-MM-DD`) |
| `source_file` | string | Ruta lógica bajo `datalake/` (ej. `landing/customers_orgs.csv`) |
| `silver_ts` / `gold_ts` | timestamp | Marca de procesamiento en Silver / Gold |
| `error_reason` | string | Motivo de cuarentena (solo `quarantine/`) |
| `quarantine_ts` | timestamp | Momento de aislamiento en cuarentena |

---

## Landing (raw inmutable)

| Dataset | Formato | Grano | Clave natural |
|---|---|---|---|
| `customers_orgs.csv` | CSV | 1 fila / org | `org_id` |
| `users.csv` | CSV | 1 fila / usuario | `user_id` |
| `billing_monthly.csv` | CSV | 1 fila / factura | `invoice_id` |
| `resources.csv` | CSV | 1 fila / recurso | `resource_id` |
| `support_tickets.csv` | CSV | 1 fila / ticket | `ticket_id` |
| `marketing_touches.csv` | CSV | 1 fila / touch | `touch_id` |
| `nps_surveys.csv` | CSV | 1 fila / encuesta | `(org_id, survey_date)` |
| `usage_events_stream/*.jsonl` | JSONL | 1 fila / evento | `event_id` |

### `usage_events` (JSONL) — campos de negocio

| Columna | Tipo landing | Notas |
|---|---|---|
| `event_id` | string | Obligatorio; dedupe en Bronze streaming |
| `timestamp` | string | Parseado a `event_ts` en Bronze |
| `org_id`, `resource_id` | string | Joins en Silver |
| `service`, `region`, `metric`, `unit` | string | Normalizados (trim, lower, aliases) |
| `value` | string/double | Cast con fallback en Bronze |
| `cost_usd_increment` | double | Dominio válido ≥ −0.01; anomalías en Silver |
| `schema_version` | int | 1 histórico; 2 con `carbon_kg`, `genai_tokens` |
| `carbon_kg`, `genai_tokens` | double/long | Nullable en v1; presentes en v2 |

---

## Bronze

Partición batch: `ingest_date`. Eventos tras reparquet: `usage_date`, `service`.

### Maestros (batch)

Mismas columnas de negocio que landing, tipadas (`boolean`, `date`, `double` donde aplica) + columnas técnicas. Dedupe por clave natural conservando `ingest_ts` más reciente.

### `usage_events` (streaming)

| Columna | Tipo | Descripción |
|---|---|---|
| `event_ts` | timestamp | Tiempo del evento |
| `usage_date` | date | `to_date(event_ts)`; partición post-reparquet |
| `value` | double | Valor numérico del métrico |
| `is_late_arrival` | boolean | `event_latency_sec` > 600 s (10 min) |
| `event_latency_sec` | long | `ingest_ts − event_ts` |
| `ingest_ts` | timestamp | Replay: `event_ts`; producción: `now()` |

---

## Silver

### Maestros conformados

Texto normalizado (`hq_region`, `service`, `channel`, `severity`, …). Quarantine en `quarantine/silver/<dataset>/` por PK nula u `org_id` huérfano.

### `usage_events` (grano evento)

Partición: `usage_date`.

| Columna | Tipo | Descripción |
|---|---|---|
| `event_id` | string | Único en válidos |
| `org_name`, `org_plan_tier` | string | Join `customers_orgs` |
| `resource_service`, `resource_region` | string | Join `resources` (gana sobre evento) |
| `cost_usd_increment` | double | Costo incremental del evento |
| `requests`, `cpu_hours`, `storage_gb_hours` | double | Features derivadas de `metric`/`value` |
| `genai_tokens`, `carbon_kg` | long/double | `coalesce(0)` para schema v1 |
| `org_user_count`, `org_active_user_count` | int | Stats de `users` por org |
| `is_cost_anomaly` | boolean | OR de z-score, MAD, percentiles, p99×2 |
| `is_cost_anomaly_*` | boolean | Flag por método |
| `is_late_arrival` | boolean | Heredado de Bronze; late > 30 min → quarantine |

**Balance:** `bronze_rows = silver_valid + quarantine` (cuarentena deduplicada por fila fuente).

### `org_service_daily`

Grano: `(org_id, usage_date, service)`.

| Columna | Descripción |
|---|---|
| `daily_cost_usd` | Suma `cost_usd_increment` |
| `requests`, `cpu_hours`, `storage_gb_hours` | Sumas por métrica |
| `genai_tokens`, `carbon_kg` | Sumas |
| `event_count` | Eventos agregados |
| `has_cost_anomaly` | Algún evento anómalo en el grano |

---

## Gold (marts Parquet)

| Mart | Grano | Partición | Servido en Astra |
|---|---|---|---|
| `org_daily_usage_by_service` | org, día, servicio | `usage_date` | Sí (#1) |
| `org_top_services_by_cost` | org, `period_end`, rank | `period_end` | Sí (#2) |
| `revenue_by_org_month` | org, mes | `billing_month` | Sí (#4) |
| `tickets_by_org_date` | org, día, severidad | `ticket_date` | Sí (#3) |
| `genai_tokens_by_org_date` | org, día | `usage_date` | Sí (#5) |
| `cost_anomaly_mart` | org, día, servicio | `usage_date` | No (analítica interna) |
| `nps_by_org_date` | org, `survey_date` | `survey_date` | No |
| `marketing_touches_by_org_channel` | org, día, canal | `touch_date` | No |

### Métricas Gold destacadas

| Mart | Columnas clave |
|---|---|
| FinOps diario | `total_daily_cost_usd`, `total_requests`, `total_genai_tokens`, `has_cost_anomaly` |
| Top servicios | `accumulated_cost_usd`, `rank`, `period_start`, `period_end` (ventana 14 días) |
| Revenue | `subtotal_usd`, `credits_usd`, `taxes_usd`, `revenue_usd` (FX vía `exchange_rate_to_usd`) |
| Tickets | `ticket_count`, `sla_breach_rate`, `avg_csat` |
| GenAI | `total_genai_tokens`, `estimated_cost_usd` (= suma `cost_usd_increment` en eventos GenAI) |
| Anomalías | `anomaly_score`, `anomaly_event_count`, `has_cost_anomaly` |

---

## AstraDB (`cloud_analytics`)

Modelado query-first; PK compuesta según patrón de consulta.

| Tabla | Partition key | Clustering | Consulta |
|---|---|---|---|
| `org_daily_usage_by_service` | `(org_id)` | `usage_date DESC`, `service` | #1 |
| `org_top_services_by_cost` | `(org_id, period_end)` | `rank ASC`, `service` | #2 |
| `tickets_by_org_date` | `(org_id, severity)` | `ticket_date DESC` | #3 |
| `revenue_by_org_month` | `(org_id)` | `billing_month DESC` | #4 |
| `genai_tokens_by_org_date` | `(org_id)` | `usage_date DESC` | #5 |

Carga: `readStream.parquet` → `foreachBatch` → prepared INSERT (upsert por PK). Top-N: DELETE partición `(org_id, period_end)` antes de INSERT.

---

## Conteos de referencia (corrida limpia 2026-06-13)

| Capa | Métrica | Valor |
|---|---|---|
| Bronze batch | maestros | 80 + 800 + 240 + 400 + 1000 + 1500 + 92 |
| Bronze streaming | eventos únicos | 43.200 |
| Silver | válidos + cuarentena | 40.956 + 2.244 = 43.200 |
| Gold servidos | FinOps / top / revenue / tickets / genai | 12.108 / 258 / 240 / 984 / 1.235 |

Ver `LOG_DECISIONES.md` para umbrales (watermark, late data, anomalías) y trade-offs.
