{{
    config(
        materialized='incremental',
        unique_key='order_id',
        incremental_strategy='merge',
        partition_by={
            "field": "order_date",
            "data_type": "date"
        }
    )
}}

select
    order_id,
    customer_id as customer_key,
    product_id as product_key,
    campaign_id as campaign_key,
    region_id,
    order_date,
    format_date('%d-%b-%Y', order_date) as order_date_formatted,
    quantity,
    unit_price,
    discount,
    shipping_cost,
    round(quantity * unit_price * (1 - discount / 100), 2) as revenue,
    payment_method,
    delivery_days,
    returned_flag,
    order_status,
    warehouse_region,
    customer_rating,
    source_system,
    loaded_at
from {{ ref('int_orders_unioned') }}

{% if is_incremental() %}
where loaded_at > (select max(loaded_at) from {{ this }})
{% endif %}
