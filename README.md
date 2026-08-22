# 🛒 M3 Ecom Retail — Cloud-Native Data Platform on GCP

**In one sentence:** This project takes retail order data — arriving both as
bulk historical files and as live, one-at-a-time streaming events — and turns
them into clean, deduplicated, governed tables in BigQuery that analysts can
query and trust.

**Built on:** Google Cloud Platform · **Project ID:** `m3-ecom` · **Region:** `asia-south2`

---

## Architecture at a glance

```
Batch:   data/batch/*.csv --> batch_ingest.py --> Parquet --> GCS --> BigQuery (MERGE, dedup)
Stream:  data/stream/*.csv --> stream_publish.py --> Pub/Sub --> Dataflow --> BigQuery
                                                                      |
                                    raw.orders_batch / raw.orders_stream
                                    raw.orders_batch_errors / raw.orders_stream_errors
                                                     |
                                          dbt: staging -> intermediate -> marts
                                                     |
                              processed.stg_* (views)   reporting.dim_*, fact_orders, rpt_sales_summary
```

## Repository structure

```
ingestion/            batch_ingest.py, stream_publish.py, dataflow_pipeline.py
sql/                  raw table DDL, RLS, analytics + verification + reconciliation queries
dbt_project/          staging -> intermediate -> marts (dims, fact, reporting view) + tests
data/batch/            source CSVs for batch ingestion (customers, products, campaigns, orders)
data/stream/            source CSV that stream_publish.py replays as Pub/Sub messages
docs/                  gcp_commands_used.md, data_governance.md
Dockerfile             batch_ingest.py packaged for Cloud Run
```

## Local setup

```powershell
python -m venv venv
.\venv\Scripts\activate
pip install -r requirements.txt
pip install dbt-bigquery

gcloud auth login
gcloud auth application-default login
gcloud config set project m3-ecom

copy ingestion\.env.example ingestion\.env   # then fill in your own values
```

## Running each piece

```powershell
# Batch ingestion (CSV -> Parquet -> GCS -> BigQuery MERGE)
cd ingestion
python batch_ingest.py

# Streaming ingestion — publish sample messages to Pub/Sub
python stream_publish.py

# Streaming ingestion — consume via Dataflow, write to BigQuery
python dataflow_pipeline.py --runner=DataflowRunner

# dbt: build staging/intermediate/marts models + run tests
cd ..\dbt_project
dbt run
dbt test
dbt docs generate
dbt docs serve
```

## Data quality & reliability

- **Duplicates:** batch uses a staging-table + `MERGE` (idempotent, keyed on
  `order_id`); streaming dedupes within a job run via
  `KeyByOrderId -> GroupByKey -> KeepFirstPerKey`, with
  `sql/05_stream_dedup_reconciliation.sql` as a safety net against
  cross-job-restart duplicates.
- **Invalid records:** never dropped silently — routed to
  `raw.orders_batch_errors` / `raw.orders_stream_errors` with the exact
  reason (missing field, bad type, invalid date, business-rule violation).
- **Late-arriving data:** processed normally within the allowed-lateness
  window; anything later than that is explicitly routed to the errors table
  instead of being silently dropped by Beam's windowing.

See `docs/data_governance.md` for the full write-up (including how to prove
both of the above), and `docs/gcp_commands_used.md` for every GCP resource
and IAM command used to build this.

## Governance

Datasets are registered in Dataplex (`ecom-lake` with raw/curated zones),
row-level security restricts `fact_orders` by `warehouse_region`, and
column-level security (policy tags) restricts `revenue` / `unit_price` /
`list_price` / `profit_margin_pct` to an authorized role — see
`docs/data_governance.md` for the reasoning and `docs/gcp_commands_used.md`
§5, §7, §7b for the exact commands.
