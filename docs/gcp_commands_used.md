# GCP Resources — Commands Used

Project: `m3-ecom` · Region: `asia-south2`

## 1. Cloud Storage

```powershell
gcloud storage buckets create gs://m3-ecom-batch-data --location=asia-south2
gcloud storage buckets create gs://m3-ecom-dataflow-stream --location=asia-south2
```

## 2. BigQuery datasets

```powershell
bq mk --location=asia-south2 --dataset m3-ecom:raw
bq mk --location=asia-south2 --dataset m3-ecom:processed
bq mk --location=asia-south2 --dataset m3-ecom:reporting
```

```powershell
bq query --use_legacy_sql=false < sql\01_create_raw_tables.sql
```

## 3. Pub/Sub

```powershell
gcloud pubsub topics create orders-topic
gcloud pubsub subscriptions create orders-topic-sub --topic=orders-topic
```

## 4. Dataflow (streaming pipeline)

```powershell
cd ingestion
python dataflow_pipeline.py --runner DataflowRunner --project m3-ecom --region asia-south2 --temp_location gs://m3-ecom-dataflow-stream/temp
```

For local development/testing, DirectRunner is used instead (the default):

```powershell
python dataflow_pipeline.py
```

## 5. Dataplex — registering the raw/staging/mart datasets

```powershell
gcloud dataplex lakes create ecom-lake --location=asia-south2 --display-name="E-Commerce Data Lake"
gcloud dataplex zones create ecom-raw-zone --lake=ecom-lake --location=asia-south2 --type=RAW --resource-location-type=SINGLE_REGION
gcloud dataplex zones create ecom-curated-zone --lake=ecom-lake --location=asia-south2 --type=CURATED --resource-location-type=SINGLE_REGION
```

Zones alone don't register anything — each BigQuery dataset must be attached
as an **asset** for Dataplex to catalog and discover it:

```powershell
gcloud dataplex assets create raw-orders-data `
  --location=asia-south2 --lake=ecom-lake --zone=ecom-raw-zone `
  --resource-type=BIGQUERY_DATASET `
  --resource-name=projects/m3-ecom/datasets/raw `
  --discovery-enabled

gcloud dataplex assets create processed `
  --location=asia-south2 --lake=ecom-lake --zone=ecom-curated-zone `
  --resource-type=BIGQUERY_DATASET `
  --resource-name=projects/m3-ecom/datasets/processed `
  --discovery-enabled

gcloud dataplex assets create reporting `
  --location=asia-south2 --lake=ecom-lake --zone=ecom-curated-zone `
  --resource-type=BIGQUERY_DATASET `
  --resource-name=projects/m3-ecom/datasets/reporting `
  --discovery-enabled
```

After creation, each asset runs a discovery scan (scheduled automatically,
or triggered on-demand from the console) that catalogs every table/view in
the dataset — this is the evidence for "View metadata using Dataplex."

## 6. Artifact Registry + Cloud Run (batch ingestion job)

```powershell
gcloud artifacts repositories create m3-repo --repository-format=docker --location=asia-south2

gcloud builds submit --tag asia-south2-docker.pkg.dev/m3-ecom/m3-repo/batch-ingest .

gcloud run jobs create batch-ingest-job `
  --image asia-south2-docker.pkg.dev/m3-ecom/m3-repo/batch-ingest `
  --region asia-south2 `
  --set-env-vars GCP_PROJECT_ID=m3-ecom,BATCH_BUCKET_NAME=m3-ecom-batch-data,BQ_RAW_DATASET=raw

gcloud run jobs execute batch-ingest-job --region asia-south2
```

## 7. Row-Level Security

```powershell
bq query --use_legacy_sql=false < sql\03_row_level_security.sql
```

## 7b. Column-Level Security (applied together with RLS — stretch goal)

Create a policy tag taxonomy and tag the financially sensitive columns
(`revenue`, `unit_price` on `fact_orders`; `list_price`, `profit_margin_pct`
on `dim_product`) so only users granted the `fineGrainedReader` role on
the taxonomy can see their values — everyone else sees `NULL`.

```powershell
gcloud data-catalog taxonomies create `
  --display-name="ecom_sensitive_finance" `
  --location=asia-south2 `
  --description="Policy tags for financially sensitive columns"

gcloud data-catalog taxonomies policy-tags create `
  --taxonomy="<TAXONOMY_ID from previous command>" `
  --display-name="Sensitive_Financial" `
  --location=asia-south2
```

Then in the BigQuery console: open `reporting.fact_orders` → schema → edit
`revenue` and `unit_price` → **Add policy tag** → select `Sensitive_Financial`.
Repeat for `list_price` and `profit_margin_pct` on `reporting.dim_product`.

Grant read access to the demo analyst:

```powershell
gcloud data-catalog taxonomies policy-tags add-iam-policy-binding `
  <POLICY_TAG_ID> `
  --member="user:dharmendrasharma1973@gmail.com" `
  --role="roles/datacatalog.categoryFineGrainedReader"
```

*(Screenshot of the tagged schema + a query showing masked values for an
un-granted user to be attached separately.)*

## 7c. Dataset lineage (via dbt)

Every model uses `{{ ref(...) }}` / `{{ source(...) }}`, so dbt tracks
exact column-level lineage automatically — this is the primary lineage
evidence for this project, in addition to whatever Dataplex shows at the
dataset level.

```powershell
cd dbt_project
dbt docs generate
dbt docs serve
```

This opens an interactive DAG showing `raw.orders_batch` /
`raw.orders_stream` → `stg_orders_batch` / `stg_orders_stream` →
`int_orders_unioned` → `fact_orders` / `dim_customer` / `dim_product` /
`dim_campaign` → `rpt_sales_summary`.

## 7d. Cloud Logging

`batch_ingest.py`, `stream_publish.py`, and `dataflow_pipeline.py` all use
Python's standard `logging` module. When `batch_ingest.py` runs inside
Cloud Run (see step 6), its stdout is automatically captured by Cloud
Logging with no extra setup — searchable under **Logs Explorer** filtered
to `resource.type="cloud_run_job"`. Dataflow jobs similarly stream worker
logs to Cloud Logging by default under `resource.type="dataflow_step"`.

## 8. Verification

```powershell
bq query --use_legacy_sql=false < sql\04_verification_queries.sql
```

## 9. IAM permissions actually required for the Dataflow worker service account

These were needed beyond the default project Editor role — discovered by
working through real `JOB_STATE_FAILED` errors, not assumed upfront.
`PROJECT_NUMBER` below is the numeric project number (`gcloud projects
describe m3-ecom --format="value(projectNumber)"`).

```powershell
# Dataflow's control-plane service agent needs to read/write the staging bucket
gcloud storage buckets add-iam-policy-binding gs://m3-ecom-dataflow-stream `
  --member="serviceAccount:service-PROJECT_NUMBER@dataflow-service-producer-prod.iam.gserviceaccount.com" `
  --role="roles/storage.objectAdmin"

# The worker itself (Compute Engine default SA) needs the same
gcloud storage buckets add-iam-policy-binding gs://m3-ecom-dataflow-stream `
  --member="serviceAccount:PROJECT_NUMBER-compute@developer.gserviceaccount.com" `
  --role="roles/storage.objectAdmin"

gcloud projects add-iam-policy-binding m3-ecom `
  --member="serviceAccount:PROJECT_NUMBER-compute@developer.gserviceaccount.com" `
  --role="roles/dataflow.worker"

gcloud projects add-iam-policy-binding m3-ecom `
  --member="serviceAccount:PROJECT_NUMBER-compute@developer.gserviceaccount.com" `
  --role="roles/bigquery.dataEditor"

# Pub/Sub: needed at project level, not just on the subscription, because
# a custom event-time watermark (timestamp_attribute="event_timestamp")
# makes Dataflow create an internal tracking subscription on the topic,
# which requires topic-level create permission, not just subscribe rights.
gcloud projects add-iam-policy-binding m3-ecom `
  --member="serviceAccount:PROJECT_NUMBER-compute@developer.gserviceaccount.com" `
  --role="roles/pubsub.editor"
```

## 10. Stream duplicate reconciliation (safety net for job restarts)

See `docs/data_governance.md` → "Reliability: idempotency, job restarts,
and late-arriving data" for why this is needed.

```powershell
bq query --use_legacy_sql=false < sql\05_stream_dedup_reconciliation.sql
```
