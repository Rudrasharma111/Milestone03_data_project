-- Demo analyst can only see orders from the West Hub warehouse region.
-- In production, replace the single email with an IAM group per region
-- (e.g. "group:west-hub-analysts@yourcompany.com").
CREATE ROW ACCESS POLICY west_hub_only_policy
ON `m3-ecom.reporting.fact_orders`
GRANT TO ("user:dharmendrasharma1973@gmail.com")
FILTER USING (warehouse_region = "West Hub");
