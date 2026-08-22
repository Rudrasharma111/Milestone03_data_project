-- Revenue by product category (last 90 days)
SELECT
    p.category,
    COUNT(DISTINCT f.order_id) AS order_count,
    ROUND(SUM(f.revenue), 2) AS total_revenue
FROM `m3-ecom.reporting.fact_orders` f
JOIN `m3-ecom.reporting.dim_product` p ON f.product_key = p.product_key
WHERE f.order_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
GROUP BY p.category
ORDER BY total_revenue DESC;

-- Top 10 customers by lifetime revenue
SELECT
    c.customer_name,
    c.membership_type,
    COUNT(DISTINCT f.order_id) AS order_count,
    ROUND(SUM(f.revenue), 2) AS lifetime_revenue
FROM `m3-ecom.reporting.fact_orders` f
JOIN `m3-ecom.reporting.dim_customer` c ON f.customer_key = c.customer_key
GROUP BY c.customer_name, c.membership_type
ORDER BY lifetime_revenue DESC
LIMIT 10;

-- Campaign effectiveness — revenue attributed vs organic
SELECT
    COALESCE(camp.campaign_name, 'No Campaign (Organic)') AS campaign_name,
    COUNT(DISTINCT f.order_id) AS order_count,
    ROUND(SUM(f.revenue), 2) AS total_revenue,
    ROUND(AVG(f.discount), 2) AS avg_discount_pct
FROM `m3-ecom.reporting.fact_orders` f
LEFT JOIN `m3-ecom.reporting.dim_campaign` camp ON f.campaign_key = camp.campaign_key
GROUP BY campaign_name
ORDER BY total_revenue DESC;

-- Return rate by warehouse region
SELECT
    warehouse_region,
    COUNT(*) AS total_orders,
    SUM(returned_flag) AS returned_orders,
    ROUND(SUM(returned_flag) / COUNT(*) * 100, 2) AS return_rate_pct
FROM `m3-ecom.reporting.fact_orders`
GROUP BY warehouse_region
ORDER BY return_rate_pct DESC;

-- Batch vs streaming volume comparison
SELECT
    source_system,
    COUNT(*) AS order_count,
    ROUND(SUM(revenue), 2) AS total_revenue
FROM `m3-ecom.reporting.fact_orders`
GROUP BY source_system;
