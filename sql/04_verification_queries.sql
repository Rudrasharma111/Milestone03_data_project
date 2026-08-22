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
