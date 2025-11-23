 --data check

 SELECT DISTINCT gender FROM gold.dim_customers


 --when we got overalapping columns like here the gender
 --this is called survivorship, when we have multiple columns from tables that hold the same information
 --basically :
 --if crm table has good value use it, if not, use the erp, else : unknown
 --this is so you always have some value
 SELECT DISTINCT
 ci.cst_gndr,
 ca.gen,
 CASE WHEN ci.cst_gndr != 'UNKNOWN' THEN ci.cst_gndr --CRM is master for gender info
 ELSE COALESCE(ca.gen, 'UNKNOWN')
 END AS new_gen
 FROM silver.crm_cust_info ci
 LEFT JOIN silver.erp_cust_az12 ca
 ON ci.cst_key = ca.cid

 --after done with survivorship, go into the big query and replace the columns with this case

--FOREIGN KEY INTEGRITY (DIMENSIONS)
SELECT * FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON c.customer_key = f.customer_key
LEFT JOIN gold.dim_products p
ON p.product_key = f.product_key
WHERE c.customer_key IS NULL


SELECT * FROM gold.dim_customers
