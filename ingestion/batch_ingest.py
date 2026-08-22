import os
from dotenv import load_dotenv
load_dotenv()

import csv
import json
import logging
import hashlib
from datetime import datetime, timezone

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from google.cloud import storage, bigquery
from pydantic import BaseModel, ValidationError, field_validator

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("batch_ingest")

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
BUCKET_NAME = os.getenv("BATCH_BUCKET_NAME")
RAW_DATASET = os.getenv("BQ_RAW_DATASET", "raw")
BATCH_INPUT_FOLDER = os.getenv("BATCH_INPUT_FOLDER", "../data/batch")

bq_client = bigquery.Client(project=PROJECT_ID)
gcs_client = storage.Client(project=PROJECT_ID)


class OrderRecord(BaseModel):
    order_id: str
    customer_id: int
    product_id: int
    region_id: int
    campaign_id: int | None = None
    order_date: str
    quantity: int
    unit_price: float
    discount: float
    shipping_cost: float
    payment_method: str
    delivery_days: int
    returned_flag: int
    order_status: str
    warehouse_region: str
    customer_rating: int
    customer_city: str
    customer_state: str

    @field_validator("order_date")
    @classmethod
    def valid_date(cls, v):
        datetime.strptime(v, "%Y-%m-%d")
        return v

    @field_validator("quantity")
    @classmethod
    def positive_qty(cls, v):
        if v <= 0:
            raise ValueError("quantity must be positive")
        return v


def _error_row_id(raw_row: str) -> str:
    return hashlib.md5(raw_row.encode("utf-8")).hexdigest()


def read_orders_csv(local_folder):
    path = os.path.join(local_folder, "orders_historical.csv")
    with open(path, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            yield row


def validate_orders(rows):
    good, bad = [], []
    for row in rows:
        raw_line = ",".join(str(v) for v in row.values())
        try:
            clean = {k: (None if v == "" else v) for k, v in row.items()}
            campaign_id = int(float(clean["campaign_id"])) if clean.get("campaign_id") else None
            record = OrderRecord(
                order_id=clean["order_id"],
                customer_id=int(clean["customer_id"]),
                product_id=int(clean["product_id"]),
                region_id=int(clean["region_id"]),
                campaign_id=campaign_id,
                order_date=clean["order_date"],
                quantity=int(clean["quantity"]),
                unit_price=float(clean["unit_price"]),
                discount=float(clean["discount"]),
                shipping_cost=float(clean["shipping_cost"]),
                payment_method=clean["payment_method"],
                delivery_days=int(clean["delivery_days"]),
                returned_flag=int(clean["returned_flag"]),
                order_status=clean["order_status"],
                warehouse_region=clean["warehouse_region"],
                customer_rating=int(clean["customer_rating"]),
                customer_city=clean["customer_city"],
                customer_state=clean["customer_state"],
            )
            good.append(record.model_dump())
        except (ValidationError, ValueError, KeyError, TypeError) as e:
            bad.append({
                "error_id": _error_row_id(raw_line),
                "raw_row": raw_line,
                "error_reason": str(e)[:500],
                "source_system": "batch",
                "logged_at": datetime.now(timezone.utc).isoformat(),
            })
    logger.info(f"Orders validated: {len(good)} good, {len(bad)} bad")
    return good, bad


def deduplicate_orders(rows):
    seen = {}
    for row in rows:
        seen[row["order_id"]] = row
    return list(seen.values())


def write_parquet(rows, local_path):
    df = pd.DataFrame(rows)
    if "campaign_id" in df.columns:
        df["campaign_id"] = df["campaign_id"].astype("Int64")
    table = pa.Table.from_pandas(df)
    pq.write_table(table, local_path)
    logger.info(f"Wrote {len(rows)} rows to {local_path}")


def upload_to_gcs(local_path, bucket_name, blob_path):
    bucket = gcs_client.bucket(bucket_name)
    blob = bucket.blob(blob_path)
    blob.upload_from_filename(local_path)
    uri = f"gs://{bucket_name}/{blob_path}"
    logger.info(f"Uploaded to {uri}")
    return uri


def merge_into_bigquery(gcs_uri, table_name, merge_keys, columns):
    staging_table_id = f"{PROJECT_ID}.{RAW_DATASET}._stg_{table_name}"
    target_table_id = f"{PROJECT_ID}.{RAW_DATASET}.{table_name}"

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    )
    load_job = bq_client.load_table_from_uri(gcs_uri, staging_table_id, job_config=job_config)
    load_job.result()
    logger.info(f"Loaded staging table {staging_table_id}")

    on_clause = " AND ".join([f"T.{k} = S.{k}" for k in merge_keys])
    update_cols = [c for c in columns if c not in merge_keys]
    update_clause = ", ".join([f"{c} = S.{c}" for c in update_cols])
    insert_cols = ", ".join(columns)
    insert_vals = ", ".join([f"S.{c}" for c in columns])

    merge_sql = f"""
        MERGE `{target_table_id}` T
        USING `{staging_table_id}` S
        ON {on_clause}
        WHEN MATCHED THEN UPDATE SET {update_clause}
        WHEN NOT MATCHED THEN INSERT ({insert_cols}) VALUES ({insert_vals})
    """
    bq_client.query(merge_sql).result()
    bq_client.delete_table(staging_table_id, not_found_ok=True)
    logger.info(f"MERGEd into {target_table_id}")


def load_errors_to_bigquery(bad_rows, error_table):
    if not bad_rows:
        return
    table_id = f"{PROJECT_ID}.{RAW_DATASET}.{error_table}"
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        schema=[
            bigquery.SchemaField("error_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("raw_row", "STRING"),
            bigquery.SchemaField("error_reason", "STRING"),
            bigquery.SchemaField("source_system", "STRING"),
            bigquery.SchemaField("logged_at", "TIMESTAMP"),
        ],
    )
    job = bq_client.load_table_from_json(bad_rows, table_id, job_config=job_config)
    job.result()
    logger.info(f"Logged {len(bad_rows)} bad rows to {table_id}")


REFERENCE_SOURCES = {
    "customers": {
        "file": "customers.csv",
        "table": "customers",
        "key": ["customer_id"],
        "int_cols": ["customer_id"],
        "float_cols": [],
        "date_cols": ["signup_date"],
    },
    "products": {
        "file": "products.csv",
        "table": "products",
        "key": ["product_id"],
        "int_cols": ["product_id", "stock_level"],
        "float_cols": ["mrp", "profit_margin_pct"],
        "date_cols": [],
    },
    "campaigns": {
        "file": "campaigns.csv",
        "table": "campaigns",
        "key": ["campaign_id"],
        "int_cols": ["campaign_id"],
        "float_cols": [],
        "date_cols": [],
    },
}


def load_reference_table(name, cfg, local_folder):
    path = os.path.join(local_folder, cfg["file"])
    df = pd.read_csv(path)
    for c in cfg["int_cols"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").astype("Int64")
    for c in cfg["float_cols"]:
        df[c] = pd.to_numeric(df[c], errors="coerce")
    for c in cfg["date_cols"]:
        df[c] = pd.to_datetime(df[c], errors="coerce").dt.date
    df["ingestion_timestamp"] = datetime.now(timezone.utc)

    before = len(df)
    df = df.dropna(subset=cfg["key"])
    if len(df) < before:
        logger.info(f"{name}: dropped {before - len(df)} rows with missing key")

    local_parquet = f"{name}.parquet"
    pa_table = pa.Table.from_pandas(df)
    pq.write_table(pa_table, local_parquet)

    gcs_uri = upload_to_gcs(local_parquet, BUCKET_NAME, f"processed/{name}_latest.parquet")
    merge_into_bigquery(gcs_uri, cfg["table"], cfg["key"], list(df.columns))
    os.remove(local_parquet)


def main():
    rows = list(read_orders_csv(BATCH_INPUT_FOLDER))
    good_rows, bad_rows = validate_orders(rows)
    good_rows = deduplicate_orders(good_rows)

    for row in good_rows:
        row["order_date"] = datetime.strptime(row["order_date"], "%Y-%m-%d").date()
        row["source_system"] = "batch"
        row["ingestion_timestamp"] = datetime.now(timezone.utc)

    if good_rows:
        write_parquet(good_rows, "orders_batch.parquet")
        gcs_uri = upload_to_gcs("orders_batch.parquet", BUCKET_NAME, "processed/orders_batch_latest.parquet")
        merge_into_bigquery(
            gcs_uri,
            "orders_batch",
            merge_keys=["order_id"],
            columns=list(good_rows[0].keys()),
        )
        os.remove("orders_batch.parquet")

    load_errors_to_bigquery(bad_rows, "orders_batch_errors")

    for name, cfg in REFERENCE_SOURCES.items():
        load_reference_table(name, cfg, BATCH_INPUT_FOLDER)

    logger.info("Batch ingestion complete.")


if __name__ == "__main__":
    main()