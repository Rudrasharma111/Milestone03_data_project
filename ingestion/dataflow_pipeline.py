import os
from dotenv import load_dotenv
load_dotenv()
import json
import logging
import hashlib
import argparse
from datetime import datetime, timezone
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions, StandardOptions
from apache_beam.transforms.window import FixedWindows
from apache_beam.io.gcp.bigquery import WriteToBigQuery, BigQueryDisposition
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
logger = logging.getLogger("dataflow_pipeline")

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
RAW_DATASET = os.getenv("BQ_RAW_DATASET", "raw")
TOPIC_ID = os.getenv("PUBSUB_TOPIC_ID", "orders-topic")
SUBSCRIPTION_ID = os.getenv("PUBSUB_SUBSCRIPTION_ID", "orders-topic-sub")
WINDOW_SECONDS = int(os.getenv("STREAM_WINDOW_SECONDS", "60"))
ALLOWED_LATENESS_SECONDS = int(os.getenv("STREAM_ALLOWED_LATENESS_SECONDS", "300"))
EARLY_TRIGGER_SECONDS = int(os.getenv("STREAM_EARLY_TRIGGER_SECONDS", "30"))

REQUIRED_FIELDS = [
    "order_id", "customer_id", "product_id", "region_id", "order_date",
    "quantity", "unit_price", "discount", "shipping_cost", "payment_method",
    "delivery_days", "returned_flag", "order_status", "warehouse_region",
    "customer_rating", "customer_city", "customer_state",
]

ORDERS_STREAM_SCHEMA = {
    "fields": [
        {"name": "order_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "customer_id", "type": "INTEGER"},
        {"name": "product_id", "type": "INTEGER"},
        {"name": "region_id", "type": "INTEGER"},
        {"name": "campaign_id", "type": "INTEGER"},
        {"name": "order_date", "type": "DATE"},
        {"name": "quantity", "type": "INTEGER"},
        {"name": "unit_price", "type": "FLOAT"},
        {"name": "discount", "type": "FLOAT"},
        {"name": "shipping_cost", "type": "FLOAT"},
        {"name": "payment_method", "type": "STRING"},
        {"name": "delivery_days", "type": "INTEGER"},
        {"name": "returned_flag", "type": "INTEGER"},
        {"name": "order_status", "type": "STRING"},
        {"name": "warehouse_region", "type": "STRING"},
        {"name": "customer_rating", "type": "INTEGER"},
        {"name": "customer_city", "type": "STRING"},
        {"name": "customer_state", "type": "STRING"},
        {"name": "source_system", "type": "STRING"},
        {"name": "event_timestamp", "type": "TIMESTAMP"},
        {"name": "ingestion_timestamp", "type": "TIMESTAMP"},
    ]
}

ERRORS_SCHEMA = {
    "fields": [
        {"name": "error_id", "type": "STRING", "mode": "REQUIRED"},
        {"name": "raw_row", "type": "STRING"},
        {"name": "error_reason", "type": "STRING"},
        {"name": "source_system", "type": "STRING"},
        {"name": "logged_at", "type": "TIMESTAMP"},
    ]
}
class ParseAndValidate(beam.DoFn):
    MAX_ALLOWED_LATENESS = WINDOW_SECONDS + ALLOWED_LATENESS_SECONDS
    def process(self, message):
        raw_text = message.decode("utf-8") if isinstance(message, bytes) else str(message)
        try:
            payload = json.loads(raw_text)
            missing = [f for f in REQUIRED_FIELDS if payload.get(f) in (None, "")]
            if missing:
                raise ValueError(f"missing required field(s): {missing}")

            payload["customer_id"] = int(payload["customer_id"])
            payload["product_id"] = int(payload["product_id"])
            payload["region_id"] = int(payload["region_id"])
            payload["campaign_id"] = (
                int(float(payload["campaign_id"])) if payload.get("campaign_id") not in (None, "") else None
            )
            payload["quantity"] = int(payload["quantity"])
            if payload["quantity"] <= 0:
                raise ValueError("quantity must be positive")
            payload["unit_price"] = float(payload["unit_price"])
            payload["discount"] = float(payload["discount"])
            payload["shipping_cost"] = float(payload["shipping_cost"])
            payload["delivery_days"] = int(payload["delivery_days"])
            payload["returned_flag"] = int(payload["returned_flag"])
            payload["customer_rating"] = int(payload["customer_rating"])
            datetime.strptime(payload["order_date"], "%Y-%m-%d")  # validates format

            event_dt = datetime.strptime(
                payload["event_timestamp"], "%Y-%m-%dT%H:%M:%S.%fZ"
            ).replace(tzinfo=timezone.utc)
            lateness_seconds = (datetime.now(timezone.utc) - event_dt).total_seconds()
            if lateness_seconds > self.MAX_ALLOWED_LATENESS:
                raise ValueError(
                    f"event arrived too late to process: {int(lateness_seconds)}s after "
                    f"event_timestamp, exceeds max allowed lateness of "
                    f"{self.MAX_ALLOWED_LATENESS}s (window={WINDOW_SECONDS}s + "
                    f"allowed_lateness={ALLOWED_LATENESS_SECONDS}s) -- would otherwise be "
                    f"silently dropped by the window"
                )

            payload["ingestion_timestamp"] = datetime.now(timezone.utc).isoformat()
            yield beam.pvalue.TaggedOutput("good", payload)

        except Exception as e:
            error_id = hashlib.md5(raw_text.encode("utf-8")).hexdigest()
            yield beam.pvalue.TaggedOutput("bad", {
                "error_id": error_id,
                "raw_row": raw_text[:2000],
                "error_reason": str(e)[:500],
                "source_system": "stream",
                "logged_at": datetime.now(timezone.utc).isoformat(),
            })


class KeyByOrderId(beam.DoFn):
    def process(self, element):
        yield (element["order_id"], element)


class KeepFirstPerKey(beam.DoFn):
    def process(self, element):
        order_id, records = element
        records = list(records)
        if not records:
            return
        chosen = sorted(records, key=lambda r: r["ingestion_timestamp"])[0]
        yield chosen


def run(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--runner", default="DirectRunner")
    known_args, pipeline_args = parser.parse_known_args(argv)

    default_gcp_args = []
    if not any(a.startswith("--project") for a in pipeline_args):
        default_gcp_args += [f"--project={PROJECT_ID}"]
    if not any(a.startswith("--region") for a in pipeline_args):
        default_gcp_args += [f"--region={os.getenv('GCP_REGION', 'asia-south2')}"]
    if not any(a.startswith("--temp_location") for a in pipeline_args):
        default_gcp_args += [f"--temp_location=gs://{os.getenv('STREAM_BUCKET_NAME')}/temp/"]
    if not any(a.startswith("--staging_location") for a in pipeline_args):
        default_gcp_args += [f"--staging_location=gs://{os.getenv('STREAM_BUCKET_NAME')}/staging/"]
    if not any(a.startswith("--job_name") for a in pipeline_args):
        default_gcp_args += ["--job_name=ecom-streaming-pipeline"]
    if not any(a.startswith("--requirements_file") for a in pipeline_args):
        default_gcp_args += ["--requirements_file=requirements-dataflow.txt"]

    options = PipelineOptions(pipeline_args + default_gcp_args)
    options.view_as(StandardOptions).streaming = True
    options.view_as(StandardOptions).runner = known_args.runner

    subscription_path = f"projects/{PROJECT_ID}/subscriptions/{SUBSCRIPTION_ID}"
    orders_table = f"{PROJECT_ID}:{RAW_DATASET}.orders_stream"
    errors_table = f"{PROJECT_ID}:{RAW_DATASET}.orders_stream_errors"

    with beam.Pipeline(options=options) as p:
        
        messages = p | "ReadFromPubSub" >> beam.io.ReadFromPubSub(
            subscription=subscription_path,
            timestamp_attribute="event_timestamp",
        )
        parsed = messages | "ParseAndValidate" >> beam.ParDo(ParseAndValidate()).with_outputs(
            "good", "bad"
        )
        good = (
            parsed.good
            | "WindowIntoFixed" >> beam.WindowInto(
                FixedWindows(WINDOW_SECONDS),
                trigger=beam.trigger.AfterWatermark(
                    early=beam.trigger.AfterProcessingTime(EARLY_TRIGGER_SECONDS),
                    late=beam.trigger.AfterCount(1),
                ),
                allowed_lateness=ALLOWED_LATENESS_SECONDS,
                accumulation_mode=beam.trigger.AccumulationMode.DISCARDING,
            )
            | "KeyByOrderId" >> beam.ParDo(KeyByOrderId())
            | "GroupByOrderId" >> beam.GroupByKey()
            | "KeepFirstPerKey" >> beam.ParDo(KeepFirstPerKey())
        )
        good | "WriteGoodToBQ" >> WriteToBigQuery(
            orders_table,
            schema=ORDERS_STREAM_SCHEMA,
            write_disposition=BigQueryDisposition.WRITE_APPEND,
            create_disposition=BigQueryDisposition.CREATE_NEVER,
        )
        parsed.bad | "WriteBadToBQ" >> WriteToBigQuery(
            errors_table,
            schema=ERRORS_SCHEMA,
            write_disposition=BigQueryDisposition.WRITE_APPEND,
            create_disposition=BigQueryDisposition.CREATE_NEVER,
        )

    logger.info("Pipeline finished / drained.")


if __name__ == "__main__":
    run()