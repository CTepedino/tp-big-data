# Cloud Provider Analytics

Pipeline de datos end-to-end para el challenge **Cloud Provider Analytics** (Big Data P2).

```text
Landing → Bronze → Silver → Gold → Serving (Cassandra/AstraDB)
```

- **Batch:** maestros CSV (`customers_orgs`, `users`, `billing_monthly`)
- **Streaming:** eventos JSONL (`usage_events_stream/*.jsonl`)
- **Serving:** mart FinOps `org_daily_usage_by_service` en AstraDB

Patrón arquitectónico: **Lambda** (batch + streaming). Decisiones técnicas en [`docs/LOG_DECISIONES.md`](docs/LOG_DECISIONES.md).

---

## Requisitos

- Python 3.11+
- Java 11+ (para PySpark)
- Dataset de la consigna en `datalake/landing/`
- Cuenta [AstraDB](https://astra.datastax.com) (para Serving)

---

## Estructura del proyecto

```text
cloud-provider-analytics/
├── cql/                          # Scripts DDL y consultas CQL
├── datalake/
│   ├── landing/                  # Fuente inmutable (CSV + JSONL)
│   ├── bronze/                   # Parquet tipificado
│   ├── silver/                   # Limpieza + enriquecimiento
│   ├── gold/                     # Marts de negocio
│   ├── quarantine/               # Registros rechazados
│   └── checkpoints/              # Structured Streaming
├── notebooks/                    # Orquestación Colab/local (01–05)
├── src/
│   ├── config.py                 # Rutas y credenciales
│   ├── cassandra/                # Cliente AstraDB
│   ├── jobs/                     # Jobs por capa
│   └── schemas/                  # StructType y umbrales
├── docs/                         # Consigna, entrega 1, log de decisiones
├── requirements.txt
└── .env.example                  # Plantilla credenciales Astra
```

---

## Setup local

```bash
cd cloud-provider-analytics

# 1. Entorno virtual
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# 2. Dataset — copiar landing/ del paquete de la consigna:
#    datalake/landing/customers_orgs.csv
#    datalake/landing/users.csv
#    datalake/landing/billing_monthly.csv
#    datalake/landing/usage_events_stream/*.jsonl

# 3. (Opcional) AstraDB — copiar .env.example → .env y completar
cp .env.example .env
export $(grep -v '^#' .env | xargs)   # o exportar manualmente
```

Variables de entorno relevantes:

| Variable | Descripción | Default |
|---|---|---|
| `DATA_ROOT` | Raíz del datalake | `./datalake` |
| `CASSANDRA_KEYSPACE` | Keyspace Astra | `cloud_analytics` |
| `ASTRA_DB_APPLICATION_TOKEN` | Token de aplicación | — |
| `ASTRA_DB_SECURE_BUNDLE_PATH` | Ruta al `.zip` del bundle | — |
| `STREAMING_WATERMARK` | Watermark replay estático | `60 days` |

---

## Quickstart — pipeline completo

Ejecutar en orden. Cada paso es idempotente.

### 1. Bronze batch (3 maestros CSV)

```bash
python -m src.jobs.bronze_batch
```

Salida: `datalake/bronze/{customers_orgs,users,billing_monthly}/`

### 2. Bronze streaming (eventos JSONL)

```bash
python -m src.jobs.bronze_streaming
```

Salida: `datalake/bronze/usage_events/` · Checkpoint: `datalake/checkpoints/usage_events_bronze/`

> El landing histórico (~60 días) usa watermark `60 days` para no descartar eventos en `availableNow`. En producción near-real-time usar `STREAMING_WATERMARK=10 minutes`.

### 3. Silver (eventos + customers_orgs)

```bash
python -m src.jobs.silver_batch
```

Salida: `datalake/silver/` · Quarantine: `datalake/quarantine/silver/usage_events/`

### 4. Gold (mart FinOps)

```bash
python -m src.jobs.gold_batch
```

Salida: `datalake/gold/org_daily_usage_by_service/` — grano `(org_id, usage_date, service)`

### 5. Serving Cassandra / AstraDB

```bash
# Preview sin credenciales
python -m src.jobs.serving_cassandra --dry-run

# Carga real (requiere token + bundle)
python -m src.jobs.serving_cassandra
```

Ejecuta DDL, carga 12.114 filas Gold y corre consultas #1 y #2.

---

## Setup AstraDB

1. Crear base en [AstraDB](https://astra.datastax.com)
2. Descargar **Secure Connect Bundle** (`.zip`)
3. Generar **Application Token** con rol Database Administrator
4. Configurar:

```bash
export CASSANDRA_KEYSPACE=cloud_analytics
export ASTRA_DB_APPLICATION_TOKEN="AstraCS:..."
export ASTRA_DB_SECURE_BUNDLE_PATH="/ruta/secure-connect-cloud-analytics.zip"
```

5. Ejecutar `python -m src.jobs.serving_cassandra`

DDL manual (alternativa): `cql/01_keyspace.cql` → `cql/02_org_daily_usage_by_service.cql`

---

## Consultas de demostración (AstraDB)

Definidas en [`cql/03_queries.cql`](cql/03_queries.cql):

**#1 — Costos y requests diarios por org y servicio (rango de fechas)**

```sql
SELECT usage_date, service, total_daily_cost_usd, total_requests
FROM cloud_analytics.org_daily_usage_by_service
WHERE org_id = 'org_rixa11dp'
  AND usage_date >= '2025-07-01'
  AND usage_date <= '2025-08-31';
```

**#2 — Top servicios por costo acumulado (últimos 14 días)**

CQL trae filas por `(org_id, usage_date, service)`; el top-N se calcula agregando por `service` en el notebook/job (`run_query_top_services_by_cost`).

---

## Notebooks (Colab o local)

| Notebook | Capa |
|---|---|
| [`01_batch_bronze.ipynb`](notebooks/01_batch_bronze.ipynb) | Bronze batch |
| [`02_streaming_bronze.ipynb`](notebooks/02_streaming_bronze.ipynb) | Bronze streaming |
| [`03_silver.ipynb`](notebooks/03_silver.ipynb) | Silver |
| [`04_gold.ipynb`](notebooks/04_gold.ipynb) | Gold |
| [`05_serving_cassandra.ipynb`](notebooks/05_serving_cassandra.ipynb) | Serving AstraDB |

En Colab: montar Drive, copiar repo + dataset, instalar dependencias y setear variables de Astra en la primera celda.

---

## Conteos de referencia (dataset provisto)

| Capa | Dataset | Filas |
|---|---|---:|
| Bronze batch | customers_orgs | 80 |
| Bronze batch | users | 800 |
| Bronze batch | billing_monthly | 240 |
| Bronze streaming | usage_events | 43.200 |
| Silver | usage_events (válidos) | 41.162 |
| Quarantine | usage_events (rechazados) | 2.038 |
| Gold | org_daily_usage_by_service | 12.114 |

---

## Checklist MVP (parcial 2)

- [x] Batch Bronze — 3 maestros CSV tipificados con `ingest_ts`, `source_file`, dedupe
- [x] Streaming Bronze — schema explícito, watermark, dedupe `event_id`, checkpoint
- [x] Silver — joins, 4 features, 3 reglas de calidad, quarantine con muestras
- [x] Gold — mart `org_daily_usage_by_service`
- [x] Cassandra — DDL, job de carga, consultas #1 y #2
- [ ] Capturas de consultas AstraDB (requiere ejecución con credenciales reales)

---

## Documentación adicional

- [`docs/LOG_DECISIONES.md`](docs/LOG_DECISIONES.md) — Lambda/Kappa, particiones, claves Cassandra, umbrales
- [`docs/Consigna parcial2.txt`](docs/Consigna%20parcial2.txt) — requisitos del MVP
- [`docs/Big Data Primer Entrega.md`](docs/Big%20Data%20Primer%20Entrega.md) — diseño arquitectónico (entrega 1)
