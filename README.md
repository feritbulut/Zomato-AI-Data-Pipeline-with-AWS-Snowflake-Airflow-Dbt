# Zomato AI Data Pipeline

**A production-style, end-to-end batch data platform** that takes 10M+ rows of Zomato-style food-delivery data from raw CSVs in a data lake all the way to AI-powered analytics you can talk to in plain English.

```
CSV Files → Amazon S3 → Snowflake (RAW) → dbt (STAGING → MARTS) → Airflow → Gemini AI → Streamlit
```
<img width="2730" height="1388" alt="Gemini_Generated_Image_624r8l624vcbr8l624r" src="https://github.com/user-attachments/assets/92121ca9-7291-4157-a01c-9e459e9c1a8f" />

Real orchestration, real medallion-architecture modeling, and a genuine AI layer sitting on top of the warehouse — not a toy notebook. This project simulates what a mid-sized food-delivery company's analytics stack actually looks like: a scheduled DAG loads and transforms the data every day, dbt tests keep it honest, and three Gemini-powered capabilities (review enrichment, RAG chat, and text-to-SQL) turn the warehouse into something a non-technical stakeholder can query directly.

---

## What makes this interesting

| | |
|---|---|
| **Real medallion architecture** | Bronze (RAW) → Silver (STAGING) → Gold (MARTS), built entirely in dbt with tests and lineage |
| **Keyless cloud security** | Snowflake reads from S3 via an IAM storage integration — zero long-lived credentials in the code |
| **Incremental at scale** | `fct_orders` and `fact_order_items` use MERGE-based incremental models, so a daily run touches only new rows, not all 10M+ |
| **LLM as a transformation step** | Free-text reviews are enriched into structured sentiment/topic columns by Gemini, then modeled by dbt like any other table |
| **RAG over your own reviews** | Ask a question in English, get an answer grounded in real customer reviews, with sources shown |
| **Text-to-SQL with a safety net** | Gemini writes the SQL, a SELECT-only guard checks it, and it runs read-only against the warehouse |
| **Fully orchestrated** | One Airflow DAG runs load → transform → enrich → AI-mart every day, no manual steps |

---

## Architecture

```
┌──────────┐   ┌────────┐   ┌─────────────┐   ┌──────────────┐   ┌─────────────┐   ┌──────────────────┐
│  SOURCE  │──▶│  LAKE  │──▶│   BRONZE    │──▶│    SILVER    │──▶│    GOLD     │──▶│      SERVE       │
│  7 CSVs  │   │   S3   │   │ Snowflake   │   │ dbt STAGING  │   │ dbt MARTS   │   │ Streamlit apps + │
│          │   │        │   │    RAW      │   │   (views)    │   │  (tables)   │   │    Snowsight     │
└──────────┘   └────────┘   └─────────────┘   └──────────────┘   └─────────────┘   └──────────────────┘
                                                       ▲                 ▲                    ▲
                                                       │                 │                    │
                                          ┌────────────┴─────────────────┴────────────────────┴──────┐
                                          │              AI LANE — Gemini API                        │
                                          │  1. LLM enrichment  → structured sentiment/topic columns  │
                                          │  2. RAG              → chat with your reviews             │
                                          │  3. Text-to-SQL      → chat with your warehouse           │
                                          └────────────────────────────┬───────────────────────────────┘
                                                                       │
                                                    ┌──────────────────┴──────────────────┐
                                                    │     Apache Airflow — one daily DAG   │
                                                    │  reload_raw → dbt_build_core →       │
                                                    │  enrich_reviews → dbt_build_ai       │
                                                    └───────────────────────────────────────┘
```

---

## Tech stack

| Layer | Technology |
|---|---|
| Language / data prep | Python, Pandas |
| Lake | Amazon S3 |
| Warehouse | Snowflake |
| Transformation | dbt (dbt-snowflake) |
| Orchestration | Apache Airflow 3 (Docker) |
| AI / LLM | Google **Gemini API** (`gemini-3.5-flash-lite` for enrichment, `gemini-embedding-001` + `gemini-3.8-flash` for RAG and text-to-SQL) |
| Apps | Streamlit |
| Security | IAM storage integration, KMS, warehouse-level network policies |

---

## What gets built

| Layer | Where | What |
|---|---|---|
| **Source** | `data/` (local) | 4 dimension CSVs (restaurants, users, food, menu) + 3 fact files: **10M orders**, **~23M order items**, **300K free-text reviews** |
| **Lake** | Amazon S3 | One bucket, `raw/<table>/` folder per CSV |
| **Bronze** | `ZOMATO.RAW` | Loaded via `COPY INTO` from S3 through a keyless storage integration |
| **Silver** | `ZOMATO.STAGING` | dbt views — cleaned, typed, renamed |
| **Gold** | `ZOMATO.MARTS` | Dimensions, incremental facts (MERGE), business marts |
| **AI** | `ZOMATO.AI` | Gemini-enriched reviews (sentiment / topic / key issue), RAG chat, text-to-SQL |
| **Orchestration** | Airflow (Docker) | One daily DAG: `reload_raw → dbt_build_core → enrich_reviews → dbt_build_ai` |

---

## Repository structure

```
├── airflow/                   # Airflow 3 on Docker
│   ├── Dockerfile             #   Snowflake provider + dbt in its own venv
│   ├── docker-compose.yaml    #   postgres + api-server + scheduler; creds via env vars
│   ├── example.env            #   template for SNOWFLAKE_* / GEMINI_API_KEY
│   └── dags/zomato_batch.py   #   the pipeline DAG (4 tasks)
├── zomato/                    # dbt project
│   ├── models/staging/        #   7 staging views (Silver) + sources + tests
│   ├── models/marts/          #   dims, incremental facts, business marts (Gold)
│   └── macros/                #   custom schema-name macro
├── ai/                        # AI layer
│   ├── enrich_reviews.py      #   Gemini enrichment → ZOMATO.AI.REVIEW_ENRICHED
│   ├── rag_chat.py            #   RAG — "chat with your reviews" (Streamlit)
│   ├── text_to_sql.py         #   text-to-SQL — "chat with your warehouse" (Streamlit)
│   └── example.env            #   template for the AI credentials
├── snowflake/                 # Snowflake setup SQL (run in Snowsight, in order)
│   ├── 01_setup.sql           #   warehouse, database, schemas, role
│   ├── 02_storage_integration.sql  # keyless S3 link (pairs with aws/)
│   ├── 03_stage_and_formats.sql    # external stage + CSV file format
│   ├── 04_raw_table.sql       #   RAW (Bronze) table DDL
│   └── 05_copy_into.sql       #   COPY INTO RAW from the stage
├── aws/                       # IAM policy + role trust policies for the S3 ↔ Snowflake handshake
└── docs/architecture.png      # architecture diagram
```

> `data/` (~2.3 GB of CSVs), logs, and dbt `target/` artifacts are intentionally not committed.

---

## How the pipeline works

### 1 · Data lands in S3
The seven CSVs are uploaded to `s3://<BUCKET>/raw/<table>/` — one folder per table (`restaurants/`, `users/`, `food/`, `menu/`, `orders/`, `order_items/`, `reviews/`).

### 2 · S3 → Snowflake: one keyless handshake
Snowflake reads the bucket with **no stored keys**, using a storage integration + an IAM role. The Snowflake side lives in `snowflake/02_storage_integration.sql`; the AWS JSON documents live in `aws/`:

| File | Used for |
|---|---|
| `s3-read-policy.json` | IAM policy — read-only access to the bucket |
| `snowflake-role-trust-policy-initial.json` | Placeholder trust used at creation time |
| `snowflake-role-trust-policy-final.json` | Final trust — Snowflake's IAM user ARN + external ID from `DESC INTEGRATION` |

The order matters: create the AWS policy + role → create the Snowflake `STORAGE INTEGRATION` pointing at the role ARN → `DESC INTEGRATION` to get `STORAGE_AWS_IAM_USER_ARN` and `STORAGE_AWS_EXTERNAL_ID` → paste both into the role's trust policy.

> Two hard-won lessons: the trust `Principal` must be Snowflake's IAM user ARN, **not** `:root` — and never re-run `CREATE OR REPLACE` on the integration afterward, it regenerates the external ID and breaks the trust.

### 3 · Load — `COPY INTO`
Table DDL (`snowflake/04_raw_table.sql`) matches each CSV's column order; `snowflake/05_copy_into.sql` pulls each file from the stage into `ZOMATO.RAW` tables.

### 4 · Transform — dbt (medallion)
- **Staging (Silver)** — one view per source: parse the messy restaurant dimension (`--` → null, `₹ 200` → `200`), lowercase emails, derive `is_delivered`, etc.
- **Dimensions (Gold)** — `dim_restaurants`, `dim_customer` (with age segments), `dim_food`, a generated `dim_date` calendar.
- **Facts (Gold, incremental)** — `fct_orders` and `fact_order_items` use `materialized='incremental'` with a MERGE strategy, so a re-run processes only new rows instead of rebuilding 10M+.
- **Marts (Gold)** — one table per business question: daily city revenue (GMV/AOV/cancel rate), restaurant performance, delivery SLA (p50/p90 by city & hour), review insights.
- **Tests** — `unique` / `not_null` / `relationships` / `accepted_values`; `dbt build` runs models and tests in dependency order.

### 5 · Orchestrate — Airflow
One daily DAG, `zomato_batch`, runs the whole thing as a single graph:

```
reload_raw  →  dbt_build_core  →  enrich_reviews  →  dbt_build_ai
(COPY from S3)  (dbt build + tests)  (Gemini enrichment)  (AI mart)
```

Credentials never touch the code: `docker-compose` injects `SNOWFLAKE_*` env vars (read by dbt's `profiles.yml` via `env_var()`) and an `AIRFLOW_CONN_SNOWFLAKE_DEFAULT` connection for the COPY task.

### 6 · AI layer — three capabilities, powered by Gemini
- **LLM enrichment** (`ai/enrich_reviews.py`) — LLM as a transformation step. Reads review text, asks `gemini-3.5-flash-lite` for structured JSON (sentiment label, sentiment score, topic, key issue), writes it back to `ZOMATO.AI.REVIEW_ENRICHED` — which dbt then models into `mart_review_insights` like any other table. Idempotent and sample-capped (`SAMPLE_N`) so you never pay to re-enrich the same review.
- **RAG** (`ai/rag_chat.py`) — chat with your reviews. Embeds reviews with `gemini-embedding-001`, retrieves the most similar ones for a question by cosine similarity, and generates a grounded answer with `gemini-3.8-flash` — with sources shown in an expander.
- **Text-to-SQL** (`ai/text_to_sql.py`) — chat with your warehouse. `gemini-3.8-flash` gets the marts' schema, writes Snowflake SQL for an English question, and a SELECT-only guard (keyword denylist + prefix check) validates it before it runs read-only as `DBT_ROLE`.
<img width="616" height="891" alt="Ekran görüntüsü 2026-09-26 093555" src="https://github.com/user-attachments/assets/d8a1a11c-f619-4d95-958e-922c7cccb973" />

---

## Running it

### Snowflake objects
```sql
-- warehouse ZOMATO_WH, database ZOMATO, schemas RAW/STAGING/MARTS/AI, role DBT_ROLE
-- + the S3 storage integration: run snowflake/01 → 05 in Snowsight, in order — see aws/ for the AWS side
```

### dbt
```bash
cd zomato
export SNOWFLAKE_ACCOUNT=... SNOWFLAKE_USER=... SNOWFLAKE_PASSWORD=...
dbt debug && dbt build --exclude tag:ai
```

### Airflow
```bash
cd airflow
cp example.env .env          # fill in SNOWFLAKE_*, GEMINI_API_KEY, SAMPLE_N
docker compose build && docker compose up -d
# http://localhost:8080 → un-pause zomato_batch → Trigger
```

### AI apps
```bash
export GEMINI_API_KEY=...
python ai/enrich_reviews.py
streamlit run ai/rag_chat.py      # chat with reviews
streamlit run ai/text_to_sql.py   # chat with the warehouse
```

---

## Dataset

7 CSVs — 4 dimensions (restaurants, users, food, menu) and 3 fact tables (10M orders, ~23M order items, 300K free-text reviews) — too large to commit to the repo. Download and place them under `data/` before running the pipeline.

---

## Security notes

- No long-lived AWS keys anywhere in the codebase — S3 access is entirely via the Snowflake storage integration + IAM role trust.
- Text-to-SQL runs read-only, as a dedicated `DBT_ROLE`, behind a SELECT-only guard.
- All credentials (Snowflake, Gemini) are injected via environment variables / `.env` files, never hard-coded.

---

## License

Add your license of choice here (e.g. MIT).
