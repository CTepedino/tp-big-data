# Cloud Provider Analytics

Pipeline end-to-end con patrón **Lambda** (batch + streaming):

```text
Landing → Bronze → Silver → Gold → Serving (AstraDB)
```

| Camino | Fuente | Salida |
|---|---|---|
| Batch | `customers_orgs`, `users`, `billing_monthly`, `resources`, `support_tickets`, `marketing_touches`, `nps_surveys` (CSV) | `datalake/bronze/` |
| Streaming | `usage_events_stream/*.jsonl` | `datalake/bronze/usage_events/` |
| Serving | 5 marts Gold → 5 tablas AstraDB |

---

## Requisitos

- Python 3.11+, Java 11+
- Dataset en `datalake/landing/`
- Cuenta [AstraDB](https://astra.datastax.com) (solo para Serving)

---

## Estructura

```text
cloud-provider-analytics/
├── cql/                    # 00 DDL, 01-05 consultas demo
├── datalake/               # landing, bronze, silver, gold, quarantine, checkpoints
├── pipeline.ipynb          # orquestación end-to-end
├── src/
│   ├── jobs/               # modulos de cada capa y serving
│   ├── schemas/        
│   └── cassandra/          # cliente AstraDB, inserts, selects y demo CQL
├── docs/                   # consigna, log de decisiones
└── requirements.txt
```

---

## Setup

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

cp .env.example .env   # completar credenciales Astra (se carga automáticamente al importar src)
```

| Variable | Default | Notas |
|---|---|---|
| `DATA_ROOT` | `./datalake` (relativo al repo) | Override con env `DATA_ROOT` |
| `CASSANDRA_KEYSPACE` | `cloud_analytics` | Crear en consola Astra |
| `ASTRA_DB_APPLICATION_TOKEN` | — | Rol **Database Administrator** |
| `ASTRA_DB_SECURE_BUNDLE_PATH` | — | Ruta al `.zip` |
| `STREAMING_WATERMARK` | `60 days` | Replay estático; producción: `10 minutes` |
| `LATE_DATA_THRESHOLD_SEC` | `600` | Flag `is_late_arrival` (10 min) |
| `LATE_CATCHUP_MAX_SEC` | `1800` | Quarantine Silver si latencia > 30 min |
| `FUTURE_EVENT_TOLERANCE_SEC` | `300` | Quarantine Silver si `event_ts` > now + 5 min |
| `SPARK_SHUFFLE_PARTITIONS` | `16` | Particiones de shuffle en joins/agregaciones |
| `SPARK_TARGET_FILES_MASTER` | `1` | `coalesce` al escribir maestros (Bronze/Silver) |
| `SPARK_TARGET_FILES_EVENTS` | `8` | `repartition` en eventos y reparquet Bronze |
| `SPARK_TARGET_FILES_GOLD` | `4` | `repartition` por columna de partición en Gold |

---

## Quickstart

Ejecutar en orden (idempotente):

```bash
python -m src.jobs.bronze_batch
python -m src.jobs.bronze_streaming
python -m src.jobs.silver
python -m src.jobs.gold

python -m src.jobs.serving_cassandra              # DDL + carga Gold (foreachBatch)
python -m src.jobs.serving_cassandra --skip-load  # solo DDL (sin carga)
python -m src.jobs.serving_cassandra --demo-queries  # carga + consultas #1–#5 (CLI)
```

Consultas CQL en notebook §7 o con `--demo-queries`. Para **corrida limpia** desde landing (normalización + dedupe): ejecutar §1→§4 del notebook con `reset_state=True` en §2, o en orden los cuatro jobs anteriores y streaming con reset.

O todo junto: [`pipeline.ipynb`](pipeline.ipynb) (desde la raíz del repo)

---

## AstraDB

Usar base **Serverless (non-vector)**. Crear keyspace 'cloud_analytics'. El keyspace puede crearse durante la creación de la base, o despues

1. [astra.datastax.com](https://astra.datastax.com) → **Create Database** (non-vector)
2. **Keyspaces** → **Add Keyspace** → `cloud_analytics`
3. **Connect** → descargar Secure Connect Bundle + generar token **Database Administrator**
4. Completar `.env` y correr `python -m src.jobs.serving_cassandra`

El job crea las 5 tablas (`cql/00_create_tables.cql`) y carga los marts Gold vía **Structured Streaming `foreachBatch`**. Las consultas demo (#1–#5) están en el notebook §7 o con `--demo-queries`.

> **Carga (consigna):** Gold Parquet → `readStream.parquet` → `writeStream.foreachBatch` → prepared INSERT a Cassandra. Las consultas de demo usan `cassandra-driver`. Opcional en Spark 3.5.x: `src/cassandra/spark_connector.py` (Spark Cassandra Connector).

### Scripts CQL

| Archivo | Uso |
|---|---|
| [`cql/00_create_tables.cql`](cql/00_create_tables.cql) | DDL de 5 tablas (keyspace manual en consola) |
| [`cql/01_daily_costs_and_requests.cql`](cql/01_daily_costs_and_requests.cql) | Consulta #1 — costos/requests diarios |
| [`cql/02_top_services_by_cost.cql`](cql/02_top_services_by_cost.cql) | Consulta #2 — top-N servicios (CQL puro) |
| [`cql/03_critical_tickets_sla.cql`](cql/03_critical_tickets_sla.cql) | Consulta #3 — tickets críticos y SLA |
| [`cql/04_monthly_revenue.cql`](cql/04_monthly_revenue.cql) | Consulta #4 — revenue mensual USD |
| [`cql/05_genai_tokens_daily.cql`](cql/05_genai_tokens_daily.cql) | Consulta #5 — tokens GenAI por día |

### Troubleshooting

| Error | Solución |
|---|---|
| `Missing correct permission on cloud_analytics` | Crear keyspace en consola; token **Database Administrator** desde **Connect** |
| `Secure connect bundle not found` | Ruta absoluta en `ASTRA_DB_SECURE_BUNDLE_PATH` |
| `Authentication failed` | Regenerar token scoped a la base correcta |
| Carga lenta / parece congelada | Normal en free tier (~1–2 min); usa `--skip-load` si solo necesitas DDL |

---

## Conteos de referencia

| Capa | Filas |
|---|---:|
| Bronze batch | customers_orgs / users / billing / resources / tickets / marketing / nps | 80 / 800 / 240 / 400 / 1000 / 1500 / 92 |
| Bronze streaming (usage_events) | 43.200 |
| Silver válidos / quarantine (usage_events) | 40.956 / 2.249 |
| Silver maestros | 7 datasets (mismos conteos que Bronze) |
| Gold `org_daily_usage_by_service` | 12.108 |
| Gold `org_top_services_by_cost` | 258 (top-5 × orgs, ventana rolling) |
| Gold `revenue_by_org_month` | 240 |
| Gold `cost_anomaly_mart` | 12.108 (6.351 con anomalía) |
| Gold `tickets_by_org_date` | 984 |
| Gold `genai_tokens_by_org_date` | 1.235 |
| Gold `nps_by_org_date` | 92 (Gold-only, no Cassandra) |
| Gold `marketing_touches_by_org_channel` | 1.477 (Gold-only, no Cassandra) |

Conteos de referencia tras corrida limpia batch + streaming + Silver + Gold (2026-06-13).

Gold-only: `cost_anomaly_mart` (consigna §4 FinOps, sin consulta CQL mínima).

---

## Checklist MVP (parcial 2)

- [x] Batch + streaming Bronze
- [x] Silver (features, calidad, quarantine)
- [x] Gold (7 marts: 5 servidos + 2 CRM + `cost_anomaly_mart` Gold-only)
- [x] AstraDB (DDL, carga 5 tablas, consultas #1–#5 en código)
- [x] Performance Spark (`coalesce` / `repartition` / reparquet documentado)
- [ ] Capturas de consultas #1–#5 en CQL Console

Más detalle de decisiones técnicas: [`documentation/LOG_DECISIONES.md`](documentation/LOG_DECISIONES.md)
