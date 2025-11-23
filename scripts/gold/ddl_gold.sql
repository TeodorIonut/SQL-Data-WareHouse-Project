CREATE OR ALTER VIEW gold.dim_customers AS
 SELECT 
     ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,
     ci.cst_id AS customer_id,
     ci.cst_key AS customer_number,
     ci.cst_firstname AS first_name,
     ci.cst_lastname AS last_name,
      la.cntry AS Country,
     ci.cst_material_status AS marital_status,
     ci.cst_create_date AS create_date,
     ca.bdate AS birth_date,
      CASE WHEN ci.cst_gndr != 'UNKNOWN' THEN ci.cst_gndr --CRM is master for gender info
     ELSE COALESCE(ca.gen, 'UNKNOWN')
     END AS gender
 FROM silver.crm_cust_info ci
     LEFT JOIN silver.erp_cust_az12 ca
 ON ci.cst_key = ca.cid
     LEFT JOIN silver.erp_loc_a101 la 
 ON la.cid = ci.cst_key

 SELECT * FROM gold.dim_customers

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


 ---THE PRODUCT DIMENSION----------------
 --SELECT COUNT(*),product_key FROM ( -- check if u got duplicated data
 CREATE VIEW gold.dim_product AS
 SELECT 
 ROW_NUMBER() OVER(ORDER BY cpi.prd_start_dt) AS product_key,
 cpi.prd_id AS product_id,
 cpi.cat_id AS category_id,
 cpi.prd_key AS product_number,
 cpi.prd_nm AS product_name,
 epc.cat AS category,
 epc.subcat AS subcategory,
  epc.maintenance AS maintenance,
 cpi.prd_line AS prd_line,
 cpi.prd_cost AS product_cost,
 cpi.prd_start_dt AS product_start_date --dates usually are last columns
 FROM silver.crm_prd_info AS cpi
 LEFT JOIN silver.erp_px_cat_g1v2 epc --left join again to not lose data
 ON cpi.cat_id = epc.id
 WHERE cpi.prd_end_dt IS NULL --if you need only current products, not the old ones
-- )t
 --GROUP BY product_key
 --HAVING COUNT(*) > 1

 SELECT * FROM gold.dim_product


 ----------CREATE THE FACT SALES TABLE----------
 IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO

CREATE VIEW gold.fact_sales AS
SELECT
    pr.product_key  AS product_key,
    cu.customer_key AS customer_key,
    sd.sls_order_dt AS order_date,
     sd.sls_ord_num  AS order_number,
    sd.sls_ship_dt  AS shipping_date,
    sd.sls_due_dt   AS due_date,
    sd.sls_sales    AS sales_amount,
    sd.sls_quantity AS quantity,
    sd.sls_price    AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
    ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu
    ON sd.sls_cust_id = cu.customer_id;
GO

 --FOREIGN KEY INTEGRITY (DIMENSIONS)
 SELECT * FROM gold.fact_sales f
 LEFT JOIN gold.dim_customers c
 ON c.customer_key = f.customer_key
 LEFT JOIN gold.dim_products p
 ON p.product_key = f.product_key
 WHERE c.customer_key IS NULL


 SELECT * FROM gold.dim_customers
