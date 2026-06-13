# Cloud Provider Analytics

Pipeline de datos end-to-end para **Cloud Provider Analytics**

```text
Landing → Bronze → Silver → Gold → Serving (Cassandra/AstraDB)
```

- **Batch:** maestros CSV (`customers_orgs`, `users`, `billing_monthly`)
- **Streaming:** eventos JSONL (`usage_events_stream/*.jsonl`)
- **Serving:** mart FinOps `org_daily_usage_by_service` en AstraDB

Patrón arquitectónico: **Lambda** (batch + streaming)

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
├── notebooks/                    # Orquestación Colab/local
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
| `CASSANDRA_LOAD_CONCURRENCY` | Hilos paralelos para INSERT | `50` |
| `CASSANDRA_PROGRESS_EVERY` | Cada cuántas filas imprimir progreso | `500` |

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

# Si la tabla ya está cargada, solo consultas:
python -m src.jobs.serving_cassandra --skip-load
```

Ejecuta DDL, carga 12.114 filas Gold y corre consultas #1 y #2.

---

## Setup AstraDB (guía detallada)

### ¿Vector o normal?

Astra ofrece dos tipos de base **Serverless**. Para este proyecto necesitás una base **Cassandra clásica** con consultas CQL sobre tablas — no embeddings ni búsqueda semántica.

| Tipo en el portal | ¿Sirve para este proyecto? | Cuándo usarla |
|---|---|---|
| **Serverless (non-vector)** | **Sí — recomendada** | Tablas CQL, marts analíticos, serving query-first (nuestro caso) |
| **Serverless (vector)** | Sí, también funciona | Apps de GenAI / RAG con vector search; admite tablas no-vector, pero no la necesitamos |

**Elegí Serverless (non-vector)** para evitar confusiones. Si ya creaste una vector por error, podés usarla igual: el job conecta por `cassandra-driver` + CQL y funciona en ambos tipos.

Lo que **no** necesitás en este MVP:
- Collections de la Data API para embeddings
- `vectorize` ni modelos de embedding

### Conexión a Astra (requerida)

El Serving usa `cassandra-driver` con **dos credenciales obligatorias**:

| Variable | Descripción |
|---|---|
| `ASTRA_DB_APPLICATION_TOKEN` | Token de aplicación (`AstraCS:...`) |
| `ASTRA_DB_SECURE_BUNDLE_PATH` | Ruta al `.zip` del Secure Connect Bundle |

El bundle descarga la configuración TLS y los endpoints de tu base. Sin él, el driver no puede conectarse a Astra.

En Colab, subí el `.zip` una vez y apuntá la ruta:

```python
os.environ["ASTRA_DB_SECURE_BUNDLE_PATH"] = "/content/secure-connect-cloud-analytics.zip"
```

---

### Paso 1 — Crear cuenta y base

1. Entrá a [https://astra.datastax.com](https://astra.datastax.com) y registrate (hay tier gratuito).
2. En el menú lateral: **Databases** → **Create Database**.
3. Completá el formulario:

| Campo | Valor sugerido |
|---|---|
| **Database name** | `cloud-analytics` (o el nombre que prefieras) |
| **Deployment type** | **Serverless (non-vector)** |
| **Cloud provider** | AWS, GCP o Azure (cualquiera; elegí la región más cercana) |
| **Region** | La más cercana a donde corrés el pipeline (ej. `us-east-1`) |

4. Click **Create Database** y esperá a que el estado pase a **Active** (1–3 minutos).

### Paso 1b — Crear keyspace `cloud_analytics` (obligatorio en Astra)

Astra **no permite** `CREATE KEYSPACE` por CQL ni por el driver. Si no creás el keyspace antes, vas a ver:

```text
Unauthorized: Missing correct permission on cloud_analytics.
```

Creación manual:

1. Abrí tu base en la consola Astra.
2. Menú **Keyspaces** (o pestaña **CQL** → sección keyspaces).
3. Click **Add Keyspace**.
4. Nombre: `cloud_analytics` (debe coincidir con `CASSANDRA_KEYSPACE` en `.env`).
5. Confirmá.

> Si usás otro nombre de keyspace, actualizá `CASSANDRA_KEYSPACE` en `.env`.

---

### Paso 2 — Descargar el Secure Connect Bundle

El bundle es un `.zip` con la configuración TLS y endpoints de tu base.

1. En la consola Astra, abrí tu base `cloud-analytics`.
2. Pestaña **Connect** (o **Database Settings** → **Connect`).
3. En **Node connect** / **Drivers**, click **Download secure connect bundle**.
4. Guardá el archivo, por ejemplo:
   - Local: `~/Downloads/secure-connect-cloud-analytics.zip`
   - Colab: subilo a Drive o a `/content/secure-connect-cloud-analytics.zip`

Ese path va en `ASTRA_DB_SECURE_BUNDLE_PATH`.

---

### Paso 3 — Crear Application Token

El token reemplaza usuario/contraseña para conectarte desde Python.

1. En la consola Astra, abrí **tu base** → pestaña **Connect** → **Generate token** (así queda scoped a esa base).
2. Si usás **Settings** → **Application Tokens**, asegurate de que el rol sea **Database Administrator** para la base correcta.
3. Configuración sugerida:

| Campo | Valor |
|---|---|
| **Token name** | `cloud-analytics-pipeline` |
| **Role** | **Database Administrator** (permite CREATE KEYSPACE/TABLE e INSERT) |

4. Copiá el token (`AstraCS:...`) — **solo se muestra una vez**.

> Para solo leer datos en demos, alcanza con rol **Database User**. Para ejecutar el job de carga (DDL + INSERT), usá **Database Administrator**.

---

### Paso 4 — Configurar credenciales en el proyecto

```bash
cp .env.example .env
```

Editá `.env`:

```bash
CASSANDRA_KEYSPACE=cloud_analytics
ASTRA_DB_APPLICATION_TOKEN=AstraCS:tu_token_aqui
ASTRA_DB_SECURE_BUNDLE_PATH=/ruta/completa/secure-connect-cloud-analytics.zip
```

Cargá las variables:

```bash
set -a && source .env && set +a
# o manualmente:
export ASTRA_DB_APPLICATION_TOKEN="AstraCS:..."
export ASTRA_DB_SECURE_BUNDLE_PATH="/home/usuario/secure-connect-cloud-analytics.zip"
```

**En Google Colab** (celda inicial):

```python
import os
os.environ["ASTRA_DB_APPLICATION_TOKEN"] = "AstraCS:..."
os.environ["ASTRA_DB_SECURE_BUNDLE_PATH"] = "/content/secure-connect-cloud-analytics.zip"
# Si el bundle está en Drive:
# !cp "/content/drive/MyDrive/secure-connect-cloud-analytics.zip" /content/
```

---

### Paso 5 — Verificar Gold y cargar datos

Asegurate de tener el mart Gold generado antes de cargar:

```bash
python -m src.jobs.gold_batch   # si aún no corrido

# Preview sin conectar a Astra
python -m src.jobs.serving_cassandra --dry-run
# Debe mostrar: gold_row_count: 12114

# Carga real + consultas #1 y #2
python -m src.jobs.serving_cassandra
```

El job hace automáticamente:
1. `CREATE TABLE` (desde `cql/00_create_tables.cql`) — el keyspace debe existir en consola Astra (Paso 1b)
2. INSERT de 12.114 filas desde Gold (upsert por PK)
3. Consulta #1: costos/requests por rango de fechas
4. Consulta #2: top-5 servicios por costo acumulado

---

### Paso 6 — Consultar desde la consola Astra (CQL Console)

Para capturas de pantalla de la entrega:

1. Consola Astra → tu base → pestaña **CQL Console**.
2. Ejecutá los scripts de `cql/`:
   - [`cql/01_daily_costs_and_requests.cql`](cql/01_daily_costs_and_requests.cql)
   - [`cql/02_top_services_by_cost.cql`](cql/02_top_services_by_cost.cql)
3. Reemplazá `org_id` si querés la org con más costo (`org_53lc58dr` según el dry-run). En consola usá valores literales en lugar de `%s`.

Ejemplo consulta #1:

```sql
USE cloud_analytics;

SELECT usage_date, service, total_daily_cost_usd, total_requests
FROM org_daily_usage_by_service
WHERE org_id = 'org_rixa11dp'
  AND usage_date >= '2025-07-01'
  AND usage_date <= '2025-08-31';
```

Verificá que hay datos:

```sql
SELECT COUNT(*) FROM org_daily_usage_by_service;
-- Esperado: 12114
```

---

### DDL manual (alternativa al job)

Si preferís crear el esquema a mano en CQL Console antes de cargar:

1. Creá el keyspace `cloud_analytics` en consola Astra (Paso 1b)
2. Ejecutá [`cql/00_create_tables.cql`](cql/00_create_tables.cql)
3. Corré solo la carga: `python -m src.jobs.serving_cassandra`

---

### Modelo de tabla (referencia)

```sql
PRIMARY KEY ((org_id), usage_date, service)
WITH CLUSTERING ORDER BY (usage_date DESC, service ASC);
```

Diseño **query-first**: una partición por organización, filas ordenadas por fecha descendente y servicio. Las consultas del MVP filtran por `org_id` + rango de `usage_date` sin `ALLOW FILTERING`.

---

### Troubleshooting

| Problema | Causa probable | Solución |
|---|---|---|
| `Missing correct permission on cloud_analytics` | Keyspace no existe en Astra o token sin rol admin | Crear keyspace `cloud_analytics` en consola Astra (Paso 1b); regenerar token **Database Administrator** desde la pestaña **Connect** de **esa** base |
| `Secure connect bundle not found` | Path incorrecto al `.zip` | Verificá `ASTRA_DB_SECURE_BUNDLE_PATH` con ruta absoluta |
| `Authentication failed` | Token inválido, expirado o de otra base | Generá token nuevo desde **Connect** de tu base con rol **Database Administrator** |
| `Keyspace does not exist` | Keyspace no creado en consola | Crear `cloud_analytics` en Astra (Paso 1b) antes de correr el job |
| Script parece congelado durante carga | INSERT fila a fila + `COUNT(*)` full table en Astra | Versión actual usa carga concurrente y muestra progreso; si ya cargaste: `--skip-load` |
| Carga lenta | 12k INSERTs secuenciales | Normal en free tier; el job tarda ~1–2 min |
| Elegiste Vector por error | Tipo de base incorrecto | Funciona igual con CQL; para próximos proyectos usá non-vector |

---

## Consultas de demostración (AstraDB)

Scripts CQL:

| Archivo | Propósito |
|---|---|
| [`cql/00_create_tables.cql`](cql/00_create_tables.cql) | DDL de la tabla |
| [`cql/01_daily_costs_and_requests.cql`](cql/01_daily_costs_and_requests.cql) | Consulta #1 |
| [`cql/02_top_services_by_cost.cql`](cql/02_top_services_by_cost.cql) | Consulta #2 (agregar top-N en app) |

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

## Notebook (Colab o local)

[`notebooks/pipeline_completo.ipynb`](notebooks/pipeline.ipynb) — ejecuta el pipeline end-to-end (Bronze batch → Bronze streaming → Silver → Gold → Serving AstraDB) con evidencias y validaciones por capa.

En Colab: montar Drive, copiar repo + dataset, instalar dependencias y setear variables de Astra en la primera celda.

Para correr un paso suelto, usá los módulos CLI:

```bash
python -m src.jobs.bronze_batch
python -m src.jobs.bronze_streaming
python -m src.jobs.silver_batch
python -m src.jobs.gold_batch
python -m src.jobs.serving_cassandra
```

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

