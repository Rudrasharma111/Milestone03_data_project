select
    product_id,
    trim(category) as category,
    trim(subcategory) as subcategory,
    trim(brand) as brand,
    season_tag,
    mrp as list_price,          -- renamed for clarity in the reporting layer
    profit_margin_pct,
    trim(supplier_name) as supplier_name,
    stock_level
from {{ source('raw', 'products') }}
