CREATE ROW ACCESS POLICY west_hub_only_policy
ON `m3-ecom.reporting.fact_orders`
GRANT TO ("user:sharmarudra@gmail.com")
FILTER USING (warehouse_region = "West Hub");
