import os
from dotenv import load_dotenv
load_dotenv()
import csv
import json
import logging
import time
from datetime import datetime, timezone
from google.cloud import pubsub_v1, bigquery

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s",
)
logger = logging.getLogger("stream_publish")

PROJECT_ID = os.getenv("GCP_PROJECT_ID")
TOPIC_ID = os.getenv("PUBSUB_TOPIC_ID", "orders-topic")
RAW_DATASET = os.getenv("BQ_RAW_DATASET", "raw")
STREAM_INPUT_FOLDER = os.getenv("STREAM_INPUT_FOLDER", "../data/stream")
STREAM_LATE_OFFSET_SECONDS = int(os.getenv("STREAM_LATE_OFFSET_SECONDS", "0"))

bq_client = bigquery.Client(project=PROJECT_ID)
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)


def get_already_processed_ids():
    query = f"SELECT order_id FROM `{PROJECT_ID}.{RAW_DATASET}.orders_stream`"
    try:
        rows = bq_client.query(query).result()
        ids = {r.order_id for r in rows}
        logger.info(f"Found {len(ids)} order_id(s) already present in raw.orders_stream")
        return ids
    except Exception as e:
        logger.warning(f"Could not read existing stream table (first run?): {e}")
        return set()


def read_stream_orders():
    path = os.path.join(STREAM_INPUT_FOLDER, "orders_new.csv")
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def publish_order(row):
    payload = dict(row)
    payload["source_system"] = "stream"

    event_time = datetime.now(timezone.utc)
    if STREAM_LATE_OFFSET_SECONDS:
        from datetime import timedelta
        event_time = event_time - timedelta(seconds=STREAM_LATE_OFFSET_SECONDS)
    
    payload["event_timestamp"] = event_time.strftime("%Y-%m-%dT%H:%M:%S.") + f"{event_time.microsecond // 1000:03d}Z"

    data = json.dumps(payload).encode("utf-8")
    future = publisher.publish(
        topic_path,
        data,
        event_timestamp=payload["event_timestamp"],
    )
    message_id = future.result()
    logger.info(
        f"Sent order_id={row['order_id']} event_timestamp={payload['event_timestamp']} message_id={message_id}"
    )


def main():
    if STREAM_LATE_OFFSET_SECONDS:
        logger.info(
            f"STREAM_LATE_OFFSET_SECONDS={STREAM_LATE_OFFSET_SECONDS} -> simulating late-arriving data."
        )

    already_processed = get_already_processed_ids()
    rows = read_stream_orders()

    sent, skipped, failed = 0, 0, 0
    for row in rows:
        if row["order_id"] in already_processed:
            skipped += 1
            continue
        try:
            publish_order(row)
            sent += 1
            time.sleep(1.1) 
        except Exception as e:
            logger.error(f"Failed to publish order_id={row['order_id']}: {e}")
            failed += 1

    logger.info(f"Streaming finished. sent={sent} skipped={skipped} failed={failed}")


if __name__ == "__main__":
    main()