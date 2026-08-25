{{ config(materialized='view') }}

select
    p.category,
    f.warehouse_region,
    count(distinct f.order_id) as total_orders,
    round(sum(f.revenue), 2) as total_revenue,
    round(avg(f.revenue), 2) as avg_order_value,
    round(sum(f.returned_flag) / count(distinct f.order_id) * 100, 2) as return_rate_pct,
    round(avg(f.delivery_days), 1) as avg_delivery_days
from {{ ref('fact_orders') }} f
join {{ ref('dim_product') }} p on f.product_key = p.product_key
group by p.category, f.warehouse_region