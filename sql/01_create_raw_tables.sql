
CREATE TABLE `m3-ecom.raw.orders_batch` (
    order_id STRING NOT NULL,
    customer_id INTEGER,
    product_id INTEGER,
    region_id INTEGER,
    campaign_id INTEGER,
    order_date DATE,
    quantity INTEGER,
    unit_price FLOAT64,
    discount FLOAT64,
    shipping_cost FLOAT64,
    payment_method STRING,
    delivery_days INTEGER,
    returned_flag INTEGER,
    order_status STRING,
    warehouse_region STRING,
    customer_rating INTEGER,
    customer_city STRING,
    customer_state STRING,
    source_system STRING,
    ingestion_timestamp TIMESTAMP
)
PARTITION BY order_date;

CREATE TABLE `m3-ecom.raw.orders_stream` (
    order_id STRING NOT NULL,
    customer_id INTEGER,
    product_id INTEGER,
    region_id INTEGER,
    campaign_id INTEGER,
    order_date DATE,
    quantity INTEGER,
    unit_price FLOAT64,
    discount FLOAT64,
    shipping_cost FLOAT64,
    payment_method STRING,
    delivery_days INTEGER,
    returned_flag INTEGER,
    order_status STRING,
    warehouse_region STRING,
    customer_rating INTEGER,
    customer_city STRING,
    customer_state STRING,
    source_system STRING,
    event_timestamp TIMESTAMP,
    ingestion_timestamp TIMESTAMP
)
PARTITION BY order_date;

CREATE TABLE `m3-ecom.raw.orders_batch_errors` (
    error_id STRING NOT NULL,
    raw_row STRING,
    error_reason STRING,
    source_system STRING,
    logged_at TIMESTAMP
);

CREATE TABLE `m3-ecom.raw.orders_stream_errors` (
    error_id STRING NOT NULL,
    raw_row STRING,
    error_reason STRING,
    source_system STRING,
    logged_at TIMESTAMP
);

-- =========================================================
-- Reference / master data — batch-only, upserted by natural key
-- =========================================================

CREATE TABLE `m3-ecom.raw.customers` (
    customer_id INTEGER NOT NULL,
    customer_name STRING,
    age_group STRING,
    gender STRING,
    city STRING,
    state STRING,
    membership_type STRING,
    customer_segment STRING,
    annual_income_group STRING,
    signup_date DATE,
    ingestion_timestamp TIMESTAMP
);

CREATE TABLE `m3-ecom.raw.products` (
    product_id INTEGER NOT NULL,
    category STRING,
    subcategory STRING,
    brand STRING,
    season_tag STRING,
    mrp FLOAT64,
    profit_margin_pct FLOAT64,
    supplier_name STRING,
    stock_level INTEGER,
    ingestion_timestamp TIMESTAMP
);

CREATE TABLE `m3-ecom.raw.campaigns` (
    campaign_id INTEGER NOT NULL,
    campaign_name STRING,
    expected_performance STRING,
    ingestion_timestamp TIMESTAMP
);
