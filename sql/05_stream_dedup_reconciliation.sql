-- =============================================================================
-- Reconciliation: catches duplicates that can slip past the in-pipeline dedup
-- =============================================================================
-- WHY THIS EXISTS
-- The Dataflow pipeline dedupes order_id within a single job run using
-- KeyByOrderId -> GroupByKey -> KeepFirstPerKey. That state lives only in
-- that job's memory. If the job is CANCELLED (not drained/updated) while a
-- message has already been written to orders_stream but not yet ACKed on
-- Pub/Sub, the new job that picks up the subscription will redeliver and
-- reprocess that same message -- writing a second row for the same order_id.
-- This script finds and removes any such duplicates, keeping the earliest
-- ingestion_timestamp (the first time we actually saw the order).
--
-- WHEN TO RUN THIS
--   1. After restarting/resubmitting the streaming Dataflow job.
--   2. On a schedule (e.g. every 15 min via a scheduled query) as a
--      standing safety net.
-- =============================================================================

-- Step 1: See what would be removed (safe, read-only check)
SELECT
  order_id,
  COUNT(*) AS copies,
  ARRAY_AGG(STRUCT(ingestion_timestamp, event_timestamp) ORDER BY ingestion_timestamp) AS rows_found
FROM `m3-ecom.raw.orders_stream`
GROUP BY order_id
HAVING COUNT(*) > 1;

-- Step 2: Actually remove the duplicates, keep the earliest-seen copy per order_id
MERGE `m3-ecom.raw.orders_stream` T
USING (
  SELECT order_id, ingestion_timestamp
  FROM (
    SELECT
      order_id,
      ingestion_timestamp,
      ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY ingestion_timestamp ASC) AS rn
    FROM `m3-ecom.raw.orders_stream`
  )
  WHERE rn > 1
) D
ON T.order_id = D.order_id AND T.ingestion_timestamp = D.ingestion_timestamp
WHEN MATCHED THEN DELETE;

-- Step 3: Confirm no duplicates remain
SELECT order_id, COUNT(*) AS cnt
FROM `m3-ecom.raw.orders_stream`
GROUP BY order_id
HAVING cnt > 1;
-- Expect: 0 rows
