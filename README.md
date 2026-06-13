# Cloud Provider Analytics

Pipeline end-to-end con patrón **Lambda** (batch + streaming):

```text
Landing → Bronze → Silver → Gold → Serving (AstraDB)
```

| Camino | Fuente | Salida |
|---|---|---|
| Batch | `customers_orgs`, `users`, `billing_monthly` (CSV) | `datalake/bronze/` |
| Streaming | `usage_events_stream/*.jsonl` | `datalake/bronze/usage_events/` |
| Serving | mart Gold `org_daily_usage_by_service` | AstraDB |

---

## Requisitos

- Python 3.11+, Java 11+
- Dataset en `datalake/landing/`
- Cuenta [AstraDB](https://astra.datastax.com) (solo para Serving)

---

## Estructura

```text
cloud-provider-analytics/
├── cql/                    # 00 DDL, 01-02 consultas demo
├── datalake/               # landing, bronze, silver, gold, quarantine, checkpoints
├── notebooks/              # pipeline.ipynb
├── src/
│   ├── jobs/               # modulos de cada capa y serving
│   ├── schemas/        
│   └── cassandra/          # cliente AstraDB + queries parametrizadas
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
| `DATA_ROOT` | `./datalake` | En Colab: `/content/datalake` |
| `CASSANDRA_KEYSPACE` | `cloud_analytics` | Crear en consola Astra |
| `ASTRA_DB_APPLICATION_TOKEN` | — | Rol **Database Administrator** |
| `ASTRA_DB_SECURE_BUNDLE_PATH` | — | Ruta al `.zip` |
| `STREAMING_WATERMARK` | `60 days` | Replay estático; producción: `10 minutes` |

---

## Quickstart

Ejecutar en orden (idempotente):

```bash
python -m src.jobs.bronze_batch
python -m src.jobs.bronze_streaming
python -m src.jobs.silver_batch
python -m src.jobs.gold_batch

python -m src.jobs.serving_cassandra             # carga + consultas #1 y #2
python -m src.jobs.serving_cassandra --skip-load # solo consultas
```

O todo junto: [`notebooks/pipeline.ipynb`](notebooks/pipeline.ipynb)

---

## AstraDB

Usar base **Serverless (non-vector)**. Crear keyspace 'cloud_analytics'. El keyspace puede crearse durante la creación de la base, o despues

1. [astra.datastax.com](https://astra.datastax.com) → **Create Database** (non-vector)
2. **Keyspaces** → **Add Keyspace** → `cloud_analytics`
3. **Connect** → descargar Secure Connect Bundle + generar token **Database Administrator**
4. Completar `.env` y correr `python -m src.jobs.serving_cassandra`

El job crea la tabla (`cql/00_create_tables.cql`), carga filas Gold y ejecuta las consultas #1 y #2.

### Scripts CQL

| Archivo | Uso |
|---|---|
| [`cql/00_create_tables.cql`](cql/00_create_tables.cql) | DDL (keyspace manual en consola) |
| [`cql/01_daily_costs_and_requests.cql`](cql/01_daily_costs_and_requests.cql) | Consulta #1 — capturas en consola |
| [`cql/02_top_services_by_cost.cql`](cql/02_top_services_by_cost.cql) | Consulta #2 — top-N se agrega en app |

Los `.cql` tienen valores literales listos para CQL Console. Las queries parametrizadas del job están en `src/cassandra/queries/`.

PK: `((org_id), usage_date DESC, service ASC)` — query-first, sin `ALLOW FILTERING`.

### Troubleshooting

| Error | Solución |
|---|---|
| `Missing correct permission on cloud_analytics` | Crear keyspace en consola; token **Database Administrator** desde **Connect** |
| `Secure connect bundle not found` | Ruta absoluta en `ASTRA_DB_SECURE_BUNDLE_PATH` |
| `Authentication failed` | Regenerar token scoped a la base correcta |
| Carga lenta / parece congelada | Normal en free tier (~1–2 min); usa `--skip-load` si ya cargaste |

---

## Conteos de referencia

| Capa | Filas |
|---|---:|
| Bronze batch (orgs / users / billing) | 80 / 800 / 240 |
| Bronze streaming (usage_events) | 43.200 |
| Silver válidos / quarantine | 41.162 / 2.038 |
| Gold `org_daily_usage_by_service` | 12.114 |

---

## Checklist MVP (parcial 2)

- [x] Batch + streaming Bronze
- [x] Silver (features, calidad, quarantine)
- [x] Gold mart FinOps
- [x] AstraDB (DDL, carga, consultas #1 y #2 en código)
- [ ] Capturas de consultas en CQL Console

Más detalle de decisiones técnicas: [`docs/LOG_DECISIONES.md`](docs/LOG_DECISIONES.md)
