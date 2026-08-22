select
    product_id as product_key,
    category,
    subcategory,
    brand,
    season_tag,
    list_price,
    profit_margin_pct,
    supplier_name,
    stock_level
from {{ ref('stg_products') }}
