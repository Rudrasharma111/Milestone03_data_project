
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
