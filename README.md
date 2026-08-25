# M3 Ecom - Cloud-Native Data Platform on Google Cloud

Project ID: `m3-ecom`
Region: `asia-south2` (Delhi)

## What this project does

This project builds a small but complete data platform on Google Cloud for a
retail/e-commerce company. Order data comes from two places:

- **Batch**: historical order files (CSV, converted to Parquet) that get
  uploaded in bulk.
- **Streaming**: new orders that arrive one at a time, in real time, through
  Pub/Sub.

Both paths are cleaned, validated, deduplicated, and landed in BigQuery. On
top of BigQuery, a dbt project builds a proper layered warehouse (staging,
intermediate, marts) ending in a reporting layer that a Looker Studio
dashboard reads from. Dataplex is used to register and govern all of this,
with row-level and column-level security applied on the final reporting
tables.

Everything below is written so that a person with no prior context on this
specific project can read it top to bottom, follow along in their own GCP
project, and end up with the same result.

---

## Table of contents

1. [Architecture](#1-architecture)
2. [Repository structure](#2-repository-structure)
3. [Prerequisites](#3-prerequisites)
4. [Step 1: Project setup and enabling APIs](#4-step-1-project-setup-and-enabling-apis)
5. [Step 2: IAM permissions](#5-step-2-iam-permissions)
6. [Step 3: Cloud Storage buckets](#6-step-3-cloud-storage-buckets)
7. [Step 4: BigQuery datasets and raw tables](#7-step-4-bigquery-datasets-and-raw-tables)
8. [Step 5: Pub/Sub topic and subscription](#8-step-5-pubsub-topic-and-subscription)
9. [Step 6: Batch ingestion](#9-step-6-batch-ingestion)
10. [Step 7: Streaming ingestion with Dataflow](#10-step-7-streaming-ingestion-with-dataflow)
11. [Step 8: Data processing rules (dedup, validation, errors)](#11-step-8-data-processing-rules-dedup-validation-errors)
12. [Step 9: dbt transformation layer (processed and reporting datasets)](#12-step-9-dbt-transformation-layer-processed-and-reporting-datasets)
13. [Step 10: Dataplex governance](#13-step-10-dataplex-governance)
14. [Step 11: Row-level and column-level security](#14-step-11-row-level-and-column-level-security)
15. [Step 12: Looker Studio dashboard](#15-step-12-looker-studio-dashboard)
16. [Step 13: Cloud Run (batch as a container job)](#16-step-13-cloud-run-batch-as-a-container-job)
17. [Step 14: Cloud Logging](#17-step-14-cloud-logging)
18. [Local setup and running everything yourself](#18-local-setup-and-running-everything-yourself)
19. [Verification queries](#19-verification-queries)
20. [Milestone requirement checklist](#20-milestone-requirement-checklist)
21. [Stretch goals checklist](#21-stretch-goals-checklist)
22. [Known gaps and honest notes](#22-known-gaps-and-honest-notes)

---

## 1. Architecture

```
BATCH PATH
  data/batch/*.csv
      -> batch_ingest.py (validate, dedupe, convert to Parquet)
      -> Cloud Storage (gs://m3-ecom-batch-data)
      -> BigQuery MERGE into raw.orders_batch (idempotent)

STREAMING PATH
  data/stream/*.csv
      -> stream_publish.py (adds event_timestamp, publishes JSON messages)
      -> Pub/Sub topic: orders-topic
      -> Pub/Sub subscription: orders-topic-sub
      -> Dataflow job: ecom-streaming-pipeline (Apache Beam)
           ReadFromPubSub -> ParseAndValidate -> WindowIntoFixed
           -> KeyByOrderId -> GroupByOrderId -> KeepFirstPerKey
      -> BigQuery: raw.orders_stream (good records)
                   raw.orders_stream_errors (bad / malformed / too-late records)

TRANSFORMATION (dbt)
  raw.orders_batch, raw.orders_stream, raw.customers, raw.products, raw.campaigns
      -> processed.stg_* (staging views: cleaned, renamed, typed)
      -> processed.int_orders_unioned (batch + stream combined, deduplicated again)
      -> reporting.dim_customer, dim_product, dim_campaign, dim_date
      -> reporting.fact_orders (incremental fact table, one row per order)
      -> reporting.rpt_sales_summary (final reporting view)

GOVERNANCE AND CONSUMPTION
  Dataplex lake "e-commerce-data-lake"
      -> raw-zone (raw dataset)
      -> curated zone (processed + reporting datasets)
  Row-level security on reporting.fact_orders (by warehouse_region)
  Column-level security (policy tags) on PII / financial columns
  Looker Studio dashboard reads from reporting.rpt_sales_summary
```

---

## 2. Repository structure

```
ingestion/
  batch_ingest.py           batch pipeline: CSV -> validate -> Parquet -> GCS -> BigQuery MERGE
  stream_publish.py         reads sample stream CSV, publishes each row to Pub/Sub
  dataflow_pipeline.py      Apache Beam pipeline: Pub/Sub -> validate -> window -> dedupe -> BigQuery
  requirements-dataflow.txt Python dependencies frozen for the Dataflow worker environment
  .env.example              template for local environment variables

sql/
  01_create_raw_tables.sql            DDL for every raw.* table
  02_analytics_queries.sql            5 example analytical queries against the reporting layer
  03_row_level_security.sql           row access policy on reporting.fact_orders
  04_verification_queries.sql         row counts + a uniqueness sanity check
  05_stream_dedup_reconciliation.sql  safety-net query for cross-job-restart duplicates

dbt_project/
  models/staging/       one model per raw source, cleaned and renamed
  models/intermediate/  int_orders_unioned.sql - combines batch + stream, dedupes by order_id
  models/marts/         dim_customer, dim_product, dim_campaign, dim_date, fact_orders, rpt_sales_summary
  models/marts/schema.yml  dbt tests: not_null, unique, relationships (foreign key checks)

data/batch/    source CSVs: customers.csv, products.csv, campaigns.csv, orders_historical.csv
data/stream/   source CSV replayed as Pub/Sub messages: orders_new.csv

docs/
  gcp_commands_used.md   every gcloud/bq command used to build every resource, in order
  data_governance.md     write-up on why metadata/governance matter, and how reliability
                         (idempotency, late data) is handled in this project

screenshots/   console screenshots proving every resource below was actually created and run

Dockerfile          packages batch_ingest.py to run as a Cloud Run Job
requirements.txt    Python dependencies for local development
```

---

## 3. Prerequisites

- A Google Cloud project with billing enabled. This project used `m3-ecom`
  in region `asia-south2`.
- `gcloud` CLI installed and authenticated (`gcloud auth login`).
- Python 3.11 (matches the Dockerfile and dbt/Beam compatibility).
- `dbt-bigquery` for the transformation layer.
- A GitHub account if you plan to push this repository.

---

## 4. Step 1: Project setup and enabling APIs

```bash
gcloud auth login
gcloud auth application-default login
gcloud config set project m3-ecom

gcloud services enable \
  storage.googleapis.com \
  bigquery.googleapis.com \
  pubsub.googleapis.com \
  dataflow.googleapis.com \
  dataplex.googleapis.com \
  datacatalog.googleapis.com \
  run.googleapis.com \
  cloudbuild.googleapis.com \
  artifactregistry.googleapis.com \
  logging.googleapis.com
```

Each service maps to a piece of the platform: `storage` for Cloud Storage,
`bigquery` for the warehouse, `pubsub` and `dataflow` for the streaming
pipeline, `dataplex` and `datacatalog` for governance and policy tags, `run`
and `cloudbuild` and `artifactregistry` for the Cloud Run batch job, and
`logging` for centralized logs.

---

## 5. Step 2: IAM permissions

Two identities need permissions in this project:

1. **The person running the pipelines locally** (you) - needs enough access
   to create resources and run jobs. `roles/editor` or `roles/owner` on the
   project covers this for a learning/demo project.

2. **The Dataflow worker service account** - this is the identity that
   actually runs the streaming pipeline once it is submitted to
   `DataflowRunner`. By default it is the Compute Engine default service
   account (`PROJECT_NUMBER-compute@developer.gserviceaccount.com`). Get the
   project number first:

```bash
gcloud projects describe m3-ecom --format="value(projectNumber)"
```

Then grant the roles the worker actually needs. These were found by working
through real `JOB_STATE_FAILED` errors while building this pipeline, not
assumed in advance - each one below fixed a specific failure:

```bash
# Dataflow's own control-plane service agent needs to read/write the staging bucket
gcloud storage buckets add-iam-policy-binding gs://m3-ecom-dataflow-stream \
  --member="serviceAccount:service-PROJECT_NUMBER@dataflow-service-producer-prod.iam.gserviceaccount.com" \
  --role="roles/storage.objectAdmin"

# The worker itself (Compute Engine default service account) needs the same
gcloud storage buckets add-iam-policy-binding gs://m3-ecom-dataflow-stream \
  --member="serviceAccount:PROJECT_NUMBER-compute@developer.gserviceaccount.com" \
  --role="roles/storage.objectAdmin"

# Permission to actually run as a Dataflow worker
gcloud projects add-iam-policy-binding m3-ecom \
  --member="serviceAccount:PROJECT_NUMBER-compute@developer.gserviceaccount.com" \
  --role="roles/dataflow.worker"

# Permission to write results into BigQuery
gcloud projects add-iam-policy-binding m3-ecom \
  --member="serviceAccount:PROJECT_NUMBER-compute@developer.gserviceaccount.com" \
  --role="roles/bigquery.dataEditor"

# Pub/Sub - needed at PROJECT level, not just on the subscription. This
# pipeline sets a custom event-time watermark (timestamp_attribute =
# "event_timestamp" in dataflow_pipeline.py), which makes Dataflow create an
# internal tracking subscription on the topic itself. That requires
# topic-level create permission, not just the ability to subscribe.
gcloud projects add-iam-policy-binding m3-ecom \
  --member="serviceAccount:PROJECT_NUMBER-compute@developer.gserviceaccount.com" \
  --role="roles/pubsub.editor"
```

Replace `PROJECT_NUMBER` in every command above with the actual number
returned by the `describe` command.

For the row-level and column-level security steps later, a specific user
(the "demo analyst") is granted narrower access - that is covered in
[Step 11](#14-step-11-row-level-and-column-level-security).

---

## 6. Step 3: Cloud Storage buckets

Two buckets are needed: one for batch files, one as Dataflow's temp/staging
area for the streaming job.

```bash
gcloud storage buckets create gs://m3-ecom-batch-data --location=asia-south2
gcloud storage buckets create gs://m3-ecom-dataflow-stream --location=asia-south2
```

A third bucket, `m3-ecom_cloudbuild`, is created automatically by Cloud
Build the first time you submit a container build (see
[Step 13](#16-step-13-cloud-run-batch-as-a-container-job)) - you do not need
to create it yourself.

What each bucket is used for:

| Bucket | Purpose |
|---|---|
| `m3-ecom-batch-data` | `batch_ingest.py` writes converted Parquet files here under `processed/` before loading them into BigQuery |
| `m3-ecom-dataflow-stream` | Dataflow's required `temp_location` and `staging_location` for the streaming job's binaries and intermediate state |
| `m3-ecom_cloudbuild` | Auto-created by Cloud Build when the Docker image for Cloud Run is built |

---

## 7. Step 4: BigQuery datasets and raw tables

Three logical datasets are created: `raw`, `processed`, `reporting`.

```bash
bq mk --location=asia-south2 --dataset m3-ecom:raw
bq mk --location=asia-south2 --dataset m3-ecom:processed
bq mk --location=asia-south2 --dataset m3-ecom:reporting
```

Then the raw layer tables are created from `sql/01_create_raw_tables.sql`:

```bash
bq query --use_legacy_sql=false < sql/01_create_raw_tables.sql
```

This creates:

- `raw.orders_batch` and `raw.orders_stream` - one row per order, partitioned
  by `order_date`, one table per source system so batch and stream never
  collide before dbt intentionally combines them.
- `raw.orders_batch_errors` and `raw.orders_stream_errors` - every malformed
  or rejected row, with an `error_reason` explaining exactly why it failed.
- `raw.customers`, `raw.products`, `raw.campaigns` - reference/master data,
  loaded and kept up to date by the batch script using the same MERGE
  pattern as orders.

Note: `processed` and `reporting` start out empty. Their tables are created
later by dbt (Step 9), not by manual DDL - this keeps the transformation
logic and the table definitions in one place instead of two.

---

## 8. Step 5: Pub/Sub topic and subscription

```bash
gcloud pubsub topics create orders-topic
gcloud pubsub subscriptions create orders-topic-sub --topic=orders-topic
```

`stream_publish.py` publishes to the topic. `dataflow_pipeline.py` reads from
the subscription. Keeping them separate (rather than pulling directly from
the topic) is what lets Dataflow track its own read position independently
of any other consumer.

---

## 9. Step 6: Batch ingestion

Script: `ingestion/batch_ingest.py`

What it does, in order:

1. Reads `data/batch/orders_historical.csv` row by row.
2. Validates every row against a Pydantic model (`OrderRecord`) that checks
   types, required fields, a valid date format, and that `quantity` is
   positive.
3. Rows that fail validation are captured with the exact reason and an
   `error_id` (an md5 hash of the raw row, so the same bad row always gets
   the same ID - this is what makes error-logging idempotent on reruns).
4. Rows that pass are deduplicated in-memory by `order_id` (last one wins).
5. Good rows are written to a local Parquet file and uploaded to
   `gs://m3-ecom-batch-data/processed/orders_batch_latest.parquet`.
6. The Parquet file is loaded into a temporary staging table in BigQuery,
   then merged into `raw.orders_batch` with a `MERGE` statement keyed on
   `order_id` - this is what makes the batch load idempotent: running the
   script twice on the same file never creates duplicate rows.
7. Bad rows go through the same staging-then-MERGE pattern into
   `raw.orders_batch_errors`, keyed on `error_id`.
8. `customers.csv`, `products.csv`, and `campaigns.csv` are loaded the same
   way (staging table + MERGE) into `raw.customers`, `raw.products`,
   `raw.campaigns`.

Run it:

```bash
cd ingestion
python batch_ingest.py
```

---

## 10. Step 7: Streaming ingestion with Dataflow

Two scripts work together here.

### stream_publish.py

Reads `data/stream/orders_new.csv` and publishes each row as a JSON message
to `orders-topic`. Before publishing, it queries `raw.orders_stream` and
`raw.orders_stream_errors` for order_ids that have already been processed or
already logged as errors, and skips those - so re-running the script against
the same test file does not keep re-publishing (and re-failing) the same
rows.

Each message gets an `event_timestamp` field set to the current time (or an
artificially delayed time, if `STREAM_LATE_OFFSET_SECONDS` is set in
`.env`, to simulate late-arriving data).

```bash
python stream_publish.py
```

### dataflow_pipeline.py

An Apache Beam pipeline with these stages, matching what you will see in the
Dataflow console job graph:

1. **ReadFromPubSub** - reads from `orders-topic-sub`, using
   `event_timestamp` as the watermark-driving timestamp attribute (not the
   Pub/Sub publish time).
2. **ParseAndValidate** - parses the JSON payload, checks all required
   fields are present, casts types, validates the date format and that
   quantity is positive. It also checks whether the event is arriving too
   late (see [Step 8](#11-step-8-data-processing-rules-dedup-validation-errors)).
   Every message is tagged as either `good` or `bad`.
3. **WindowIntoFixed** - groups good records into 60-second fixed windows,
   with a 5-minute (300 second) allowed-lateness grace period and an early
   trigger every 30 seconds so results show up before a window fully closes.
4. **KeyByOrderId -> GroupByOrderId -> KeepFirstPerKey** - deduplicates
   `order_id` within each window firing, in case the same message was
   delivered more than once by Pub/Sub.
5. **WriteGoodToBQ** - appends clean records to `raw.orders_stream`.
6. **WriteBadToBQ** - appends rejected records to `raw.orders_stream_errors`.

Run it locally first with `DirectRunner` (the default) to test the logic
without spinning up Dataflow workers:

```bash
python dataflow_pipeline.py
```

Then submit it to actually run on Dataflow:

```bash
python dataflow_pipeline.py \
  --runner DataflowRunner \
  --project m3-ecom \
  --region asia-south2 \
  --temp_location gs://m3-ecom-dataflow-stream/temp
```

In the Dataflow console (Jobs -> ecom-streaming-pipeline -> Job Graph) you
should see all six stages showing "Running" with a low "Data Lag" value,
which confirms the pipeline is keeping up with incoming messages in near
real time.

---

## 11. Step 8: Data processing rules (dedup, validation, errors)

This section explains the actual logic, not just where it lives, because it
is the part most likely to come up in questions.

### Deduplication

There are three independent layers of deduplication, because each one
protects against a different failure mode:

1. **Batch**: a `MERGE` statement keyed on `order_id`. If you run the batch
   script twice, the second run updates existing rows instead of inserting
   duplicates.
2. **Streaming, within a job run**: `KeyByOrderId -> GroupByKey ->
   KeepFirstPerKey`. This catches the case where Pub/Sub redelivers the same
   message more than once while the same Dataflow job is still running.
3. **Streaming, across job restarts**: the per-window dedup above only
   exists in that job's memory. If the job is cancelled (not drained) after
   writing a row to BigQuery but before acknowledging the message on
   Pub/Sub, a new job that picks up the subscription will see it as "new"
   and write it again. `sql/05_stream_dedup_reconciliation.sql` is a
   periodic safety-net query that finds any `order_id` with more than one
   row in `raw.orders_stream` and keeps only the earliest one. It is meant
   to be run after any job restart, or on a schedule.

### Invalid record handling

Nothing is ever silently dropped. Both `batch_ingest.py` and
`dataflow_pipeline.py` route rejected rows to a dedicated errors table with
a specific, human-readable `error_reason` (for example, "quantity must be
positive" or "missing required field(s): ['order_date']").

### Late-arriving data

By default, Apache Beam silently drops any record that arrives after its
window has closed and the allowed-lateness period has passed - there is no
log line or error for this, it just disappears. To avoid that silent data
loss, `ParseAndValidate` checks each event's lateness (its `event_timestamp`
compared to the current time) before windowing even happens. If a record
would arrive too late to be processed normally, it is routed into
`raw.orders_stream_errors` with a clear reason instead of being dropped
unseen by the windowing logic. The result is that every message always ends
up in exactly one of two places: `raw.orders_stream` or
`raw.orders_stream_errors` - never neither.

To see this in action: set `STREAM_LATE_OFFSET_SECONDS` in `.env` to a value
larger than window (60s) plus allowed lateness (300s), for example `500`,
then run `stream_publish.py` and check `raw.orders_stream_errors` for rows
with a reason starting with "event arrived too late to process".

---

## 12. Step 9: dbt transformation layer (processed and reporting datasets)

The `dbt_project/` folder builds everything downstream of the raw layer.

### Staging models (`models/staging/`, materialize into the `processed` dataset)

One model per raw source: `stg_customers`, `stg_products`, `stg_campaigns`,
`stg_orders_batch`, `stg_orders_stream`. Each one simply selects from its raw
source, trims whitespace on text fields, and tags the source system.

### Intermediate model

`int_orders_unioned.sql` unions `stg_orders_batch` and `stg_orders_stream`
into one order stream, then deduplicates by `order_id` again (using
`QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY loaded_at DESC)`)
- this is a second, independent safety net on top of the dedup already done
upstream in each ingestion script.

### Mart models (`models/marts/`, materialize into the `reporting` dataset)

- `dim_customer`, `dim_product`, `dim_campaign`, `dim_date` - standard
  dimension tables.
- `fact_orders` - the central fact table. Materialized as `incremental` with
  a `merge` strategy keyed on `order_id`, partitioned by `order_date`. This
  is where the actual transformations happen:
  - renaming (`customer_id` becomes `customer_key`, etc, so fact and
    dimension tables join cleanly)
  - date formatting (`order_date_formatted` using `FORMAT_DATE('%d-%b-%Y', order_date)`)
  - a derived field: `revenue = quantity * unit_price * (1 - discount / 100) + shipping_cost`
- `rpt_sales_summary` - a `view`, not a materialized table, that aggregates
  `fact_orders` joined to `dim_product` by category and warehouse region:
  total orders, total revenue, average order value, return rate percentage,
  and average delivery days. This is the single view the Looker Studio
  dashboard reads from.

### Tests

`models/marts/schema.yml` defines dbt tests on every key column:
`not_null` and `unique` on every primary key, plus `relationships` tests
that check every foreign key in `fact_orders` actually points to a real row
in the corresponding dimension table (referential integrity, enforced as
part of the build, not just assumed).

Run it:

```bash
cd dbt_project
dbt run
dbt test
dbt docs generate
dbt docs serve
```

`dbt docs serve` opens an interactive lineage graph showing exactly how data
flows from `raw.orders_batch` / `raw.orders_stream` through staging and
intermediate models into the final dimension, fact, and reporting tables -
this is one of the two lineage views used for governance (the other is
Dataplex's own Lineage tab, described next).

---

## 13. Step 10: Dataplex governance

### Creating the lake and zones

```bash
gcloud dataplex lakes create ecom-lake \
  --location=asia-south2 --display-name="E-Commerce Data Lake"

gcloud dataplex zones create raw-zone \
  --lake=ecom-lake --location=asia-south2 --type=RAW \
  --resource-location-type=SINGLE_REGION

gcloud dataplex zones create curated \
  --lake=ecom-lake --location=asia-south2 --type=CURATED \
  --resource-location-type=SINGLE_REGION
```

### Attaching datasets as assets

Zones by themselves do not register anything. Each BigQuery dataset has to
be explicitly attached as an asset before Dataplex will discover and catalog
its tables:

```bash
gcloud dataplex assets create raw-orders-data \
  --location=asia-south2 --lake=ecom-lake --zone=raw-zone \
  --resource-type=BIGQUERY_DATASET \
  --resource-name=projects/m3-ecom/datasets/raw \
  --discovery-enabled

gcloud dataplex assets create processed \
  --location=asia-south2 --lake=ecom-lake --zone=curated \
  --resource-type=BIGQUERY_DATASET \
  --resource-name=projects/m3-ecom/datasets/processed \
  --discovery-enabled

gcloud dataplex assets create reporting \
  --location=asia-south2 --lake=ecom-lake --zone=curated \
  --resource-type=BIGQUERY_DATASET \
  --resource-name=projects/m3-ecom/datasets/reporting \
  --discovery-enabled
```

Once created, each asset runs a discovery scan (automatic on a schedule, or
triggered manually from the console) that catalogs every table and view
inside that dataset. In the console this shows up under Knowledge Catalog ->
Lakes -> e-commerce-data-lake, with both zones showing status "Active" and
"Assets requiring action: 0".

### Viewing metadata and a custom aspect type

Every cataloged table gets a Details page (Knowledge Catalog -> search for
the table, or navigate through the lake) showing system, platform, creation
time, last modification time, and an editable Overview description.

This project also defines a custom aspect type called
`data_governance_contract` with three fields: `data_steward`,
`data_sensitivity` (an enum: Public, Internal, Confidential, Restricted),
and `review_frequency`. This aspect is attached to `reporting.fact_orders`
with values such as `data_steward = "Rudra Sharma - 18-08-2026"` and
`data_sensitivity = "Confidential"` - this is what a governance contract
looks like in practice: a lightweight, structured statement of who owns a
table and how sensitive it is, attached directly to the table's metadata
instead of living in a separate spreadsheet.

The same Details page also shows a "BigQuery Policy" aspect with counts of
how many row-access policies and column-level policy tags are actually
applied to that table - useful as a quick sanity check that governance
controls described in this README are actually live, not just documented.

### Lineage

Two independent sources of lineage exist in this project:

1. **Dataplex's own Lineage tab**, on the `fact_orders` entry page in
   Knowledge Catalog - shows the BigQuery-level lineage Google infers
   automatically from job history.
2. **dbt's dependency graph** (`dbt docs generate` / `dbt docs serve`),
   which is exact and column-level because every model explicitly declares
   its dependencies with `ref()` and `source()`.

---

## 14. Step 11: Row-level and column-level security

These two controls answer different questions and are applied together on
the reporting layer, on purpose, to demonstrate that they compose correctly:

- Row-level security controls **which rows** of a table a user can see.
- Column-level security (policy tags) controls **which columns** a user can
  see, independent of which rows they already have access to.

### Row-level security

Defined in `sql/03_row_level_security.sql`, applied to
`reporting.fact_orders`:

```sql
CREATE ROW ACCESS POLICY west_hub_only_policy
ON `m3-ecom.reporting.fact_orders`
GRANT TO ("user:example-analyst@yourcompany.com")
FILTER USING (warehouse_region = "West Hub");
```

Apply it:

```bash
bq query --use_legacy_sql=false < sql/03_row_level_security.sql
```

In production, replace the single email with an IAM group per region (for
example `group:west-hub-analysts@yourcompany.com`) so you are not managing
individual users one at a time. The Dataplex "BigQuery Policy" aspect on
`fact_orders` confirms "Number of Row Access Policies: 2" once this and one
additional demo policy are both applied.

### Column-level security (policy tags)

Create a taxonomy and a policy tag for sensitive columns:

```bash
gcloud data-catalog taxonomies create \
  --display-name="ecom_pii_taxonomy" \
  --location=asia-south2 \
  --description="Policy tags for personally identifiable and financially sensitive columns"

gcloud data-catalog taxonomies policy-tags create \
  --taxonomy="<TAXONOMY_ID from the previous command>" \
  --display-name="PII - Customer Name" \
  --location=asia-south2
```

Then in the BigQuery console: open the table's schema (for example
`reporting.dim_customer`) -> Edit schema -> select the sensitive column (for
example `customer_name`) -> Add policy tag -> choose the tag just created ->
Save.

Grant read access to a specific analyst role:

```bash
gcloud data-catalog taxonomies policy-tags add-iam-policy-binding \
  <POLICY_TAG_ID> \
  --member="user:example-analyst@yourcompany.com" \
  --role="roles/datacatalog.categoryFineGrainedReader"
```

Anyone who queries `dim_customer` without that role gets `NULL` back for
`customer_name`, while every other column is unaffected. Combined with the
row-level policy above, an analyst's access is narrowed in two independent
dimensions at once: which rows, and which columns.

---

## 15. Step 12: Looker Studio dashboard

A dashboard called `M3_Dashboard` is built directly on
`reporting.rpt_sales_summary`. It contains:

- Four scorecards: Total Revenue, Total Orders, Average Delivery time,
  Average Order Value.
- A revenue-over-time line chart.
- A "Return Rate by Warehouse Region" bar chart.
- A "Category by Total Orders" pie chart.
- A "Total Revenue by Category" bar chart.

To rebuild it: open BigQuery, navigate to `reporting.rpt_sales_summary`, and
use "Explore with Looker Studio" (or "Open in" -> "Explore with Data
Studio" depending on the console version), then add charts against the
fields listed above (`total_revenue`, `total_orders`, `avg_delivery_days`,
`avg_order_value`, `category`, `warehouse_region`, `return_rate_pct`).

Because the dashboard reads from a view rather than a raw table, refreshing
the underlying dbt models automatically flows through to the dashboard on
its next refresh, with no separate export step.

---

## 16. Step 13: Cloud Run (batch as a container job)

As an alternative/complement to running `batch_ingest.py` locally, it is
packaged into a container and run as a Cloud Run Job:

```bash
gcloud artifacts repositories create m3-repo \
  --repository-format=docker --location=asia-south2

gcloud builds submit --tag asia-south2-docker.pkg.dev/m3-ecom/m3-repo/batch-ingest .

gcloud run jobs create batch-ingest-job \
  --image asia-south2-docker.pkg.dev/m3-ecom/m3-repo/batch-ingest \
  --region asia-south2 \
  --set-env-vars GCP_PROJECT_ID=m3-ecom,BATCH_BUCKET_NAME=m3-ecom-batch-data,BQ_RAW_DATASET=raw

gcloud run jobs execute batch-ingest-job --region asia-south2
```

The `Dockerfile` at the repository root copies `requirements.txt`, installs
dependencies, copies the `ingestion/` folder and `data/` folder into the
image, and sets `CMD ["python", "batch_ingest.py"]` as the entrypoint - so
the exact same script that runs locally also runs inside the container,
with no separate "production version" to maintain.

---

## 17. Step 14: Cloud Logging

No separate setup is required for this project's logs to reach Cloud
Logging:

- `batch_ingest.py`, `stream_publish.py`, and `dataflow_pipeline.py` all use
  Python's standard `logging` module for every step (validation counts,
  upload confirmations, MERGE completions, errors).
- When `batch_ingest.py` runs inside the Cloud Run Job described above, its
  stdout is automatically captured by Cloud Logging and searchable under
  Logs Explorer with `resource.type="cloud_run_job"`.
- Dataflow workers stream their logs to Cloud Logging by default under
  `resource.type="dataflow_step"` - this is where you would look if the
  streaming job's Job Graph shows a stage with a warning icon.

---

## 18. Local setup and running everything yourself

```powershell
python -m venv venv
.\venv\Scripts\activate
pip install -r requirements.txt
pip install dbt-bigquery

gcloud auth login
gcloud auth application-default login
gcloud config set project m3-ecom

copy ingestion\.env.example ingestion\.env
```

Then fill in `ingestion\.env` with your own values (defaults shown match
what this project actually used):

```
GCP_PROJECT_ID=m3-ecom
BATCH_BUCKET_NAME=m3-ecom-batch-data
STREAM_BUCKET_NAME=m3-ecom-dataflow-stream
BQ_RAW_DATASET=raw
PUBSUB_TOPIC_ID=orders-topic
PUBSUB_SUBSCRIPTION_ID=orders-topic-sub
BATCH_INPUT_FOLDER=../data/batch
STREAM_INPUT_FOLDER=../data/stream
STREAM_WINDOW_SECONDS=60
STREAM_LATE_OFFSET_SECONDS=0
```

Run each piece, in order:

```powershell
# 1. Batch ingestion (CSV -> Parquet -> GCS -> BigQuery MERGE)
cd ingestion
python batch_ingest.py

# 2. Streaming ingestion - publish sample messages to Pub/Sub
python stream_publish.py

# 3. Streaming ingestion - consume via Dataflow, write to BigQuery
python dataflow_pipeline.py --runner=DataflowRunner --project m3-ecom --region asia-south2 --temp_location gs://m3-ecom-dataflow-stream/temp

# 4. dbt: build staging/intermediate/marts models + run tests
cd ..\dbt_project
dbt run
dbt test
dbt docs generate
dbt docs serve
```

Every exact `gcloud`/`bq` command used to create every resource in this
project, in the order they were run, is also collected in
`docs/gcp_commands_used.md`.

---

## 19. Verification queries

`sql/04_verification_queries.sql` is a quick health check to run after any
end-to-end run:

```sql
SELECT COUNT(*) FROM `m3-ecom.raw.orders_batch`;
SELECT COUNT(*) FROM `m3-ecom.raw.orders_stream`;
SELECT COUNT(*) FROM `m3-ecom.raw.orders_batch_errors`;
SELECT COUNT(*) FROM `m3-ecom.raw.orders_stream_errors`;
SELECT COUNT(*) FROM `m3-ecom.raw.customers`;
SELECT COUNT(*) FROM `m3-ecom.raw.products`;
SELECT COUNT(*) FROM `m3-ecom.raw.campaigns`;
SELECT COUNT(*) FROM `m3-ecom.processed.int_orders_unioned`;
SELECT COUNT(*) FROM `m3-ecom.reporting.fact_orders`;

-- sanity check: order_id should be unique in the unioned staging layer
SELECT order_id, COUNT(*) AS cnt
FROM `m3-ecom.processed.int_orders_unioned`
GROUP BY order_id
HAVING cnt > 1;
```

Run it with:

```bash
bq query --use_legacy_sql=false < sql/04_verification_queries.sql
```

If the last query returns zero rows, deduplication is working correctly end
to end.

---

## 20. Milestone requirement checklist

| Requirement | Status | Where to look |
|---|---|---|
| Cloud Storage | Done | 2 buckets, `asia-south2` |
| Pub/Sub | Done | `orders-topic` + `orders-topic-sub`, active |
| Dataflow | Done | `ecom-streaming-pipeline`, running, 6-stage graph |
| BigQuery | Done | `raw`, `processed`, `reporting` datasets |
| Dataplex | Done | `e-commerce-data-lake`, 2 zones, 3 assets, all active |
| Batch ingestion (Parquet, GCS, BigQuery) | Done | `ingestion/batch_ingest.py` |
| Streaming ingestion (Pub/Sub, Dataflow, BigQuery) | Done | `ingestion/stream_publish.py`, `ingestion/dataflow_pipeline.py` |
| Removing duplicate records | Done | MERGE (batch), windowed dedup + reconciliation SQL (stream), `QUALIFY` dedup (dbt) |
| Handling invalid/malformed records | Done | Pydantic validation (batch), manual validation (stream) |
| Logging failed records | Done | `raw.orders_batch_errors`, `raw.orders_stream_errors` |
| Simple transformations | Done | renaming, date formatting, derived `revenue` field in `fact_orders.sql` |
| Raw / processed / reporting datasets | Done | see Step 4, Step 9 |
| Loading data, creating tables, SQL queries | Done | `sql/01_create_raw_tables.sql`, `sql/02_analytics_queries.sql` |
| At least one reporting view | Done | `reporting.rpt_sales_summary` |
| Register datasets in Dataplex | Done | Step 10 |
| View metadata via Dataplex/Data Catalog | Done | Step 10 |
| Show lineage | Done | Dataplex Lineage tab + dbt docs graph |
| Explain metadata/governance purpose | Done | `docs/data_governance.md` |

---

## 21. Stretch goals checklist

| Stretch goal | Status | Notes |
|---|---|---|
| Both Cloud Run and Dataflow | Done | Cloud Run runs batch, Dataflow runs streaming |
| Both Avro and Parquet | Not done | Only Parquet is implemented for batch files. The core requirement only asks for "Avro or Parquet," so the base requirement is met; only the stretch version (both formats) is outstanding |
| Idempotent duplicate handling | Done | MERGE (batch) + windowed dedup + reconciliation query (stream) |
| Route invalid records to a separate error table | Done | `raw.orders_batch_errors`, `raw.orders_stream_errors` |
| Handle late-arriving records | Done | explicit lateness check in `ParseAndValidate`, see Step 8 |
| Column-level and row-level security together | Done | RLS on `fact_orders`, policy tag (PII) on `dim_customer.customer_name` |
| Looker Studio dashboard | Done | `M3_Dashboard` built on `rpt_sales_summary` |
| Cloud Logging | Done | standard `logging` module in all three scripts, auto-captured |
| Explain batch vs streaming with examples | Done | see Step 7 vs Step 6, and `docs/data_governance.md` |

---

