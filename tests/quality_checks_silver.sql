----------------------------
--silver.crm_cust_info
-----------------------------

  --

 --check for nulls or duplicates in primary key
 --expectation : no result
 SELECT cst_id,COUNT(*) FROM silver.crm_cust_info
  GROUP BY cst_id
  HAVING COUNT(*) > 1 OR cst_id IS NULL

  SELECT *,ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) FROM silver.crm_cust_info
  WHERE cst_id = 29466 

  --check for unwanted Spaces
  --expectation : no results
  SELECT cst_firstname FROM silver.crm_cust_info
  WHERE cst_firstname != TRIM(cst_firstname) --trim to remove blank space

  --Data Standardization & Consistency
  SELECT DISTINCT cst_gndr 
  FROM bronze.crm_cust_info


  --first check for bad values then compute the query from below

  --crm.prod_info
  --gotta recreate it tho

   
------------------------------
--bronze.crm_prd_info--
------------------------------


  -----------BRONZE LAYER-------------
   
 --check for unwanted spaces
--expectation : no results

    SELECT prd_nm
    FROM bronze.crm_prd_info
    WHERE prd_nm != TRIM(prd_nm)
    
    --Check for NULLs or Negative Numbers
    --Expectation: No Results
    
    SELECT prd_cost
     FROM bronze.crm_prd_info
    WHERE prd_cost < 0 OR prd_cost IS NULL
    
    --Data Standardization & Consistency
    
    SELECT DISTINCT prd_line
    FROM bronze.crm_prd_info
    
    -- Check for Invalid Date Orders
    SELECT * FROM bronze.crm_prd_info
    WHERE prd_end_dr < prd_start_dt -- end date can't be earlier than start date
    --each order gotta have a start date
    --SOLUTION : END DATE = START DATE OF THE NEXT RECORD -1
    


   

---------------------------
--bronze.crm_sales_details


   ---------------BRONZE LAYER-------------------

     --check if any product key from sales details isn't in the prod info, this is the foreign key
     SELECT sls_prod_key FROM bronze.crm_sales_details
     WHERE sls_prod_key NOT IN (SELECT prd_key FROM silver.crm_prd_info)
    
     --check if any customer key from sales details isn't in the cust info, this is the foreign key
     SELECT sls_cust_id FROM bronze.crm_sales_details
     WHERE sls_cust_id NOT IN (SELECT sls_cust_id FROM silver.crm_cust_info)
    
     --CHECK FOR INVALID DATES
     SELECT 
     sls_order_dt
     FROM bronze.crm_sales_details
     WHERE sls_order_dt <= 0 OR LEN(sls_order_dt) != 8 OR sls_order_dt > 20250101
    
     --check if the dates are either not 0, or less than 8 numbers or higher than something that's not valid
     SELECT 
     sls_due_dt
     FROM bronze.crm_sales_details
     WHERE sls_due_dt <= 0 OR LEN(sls_due_dt) != 8 OR sls_due_dt > 20250101
    
     SELECT * FROM bronze.crm_sales_details
     WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt
    
     SELECT * FROM bronze.crm_sales_details
     WHERE sls_sales != sls_quantity * sls_price OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_sales IS NULL
    
     --IF SALES IS NEGATIVE,0 OR NULL,DERIVE IT USING QUANTITY AND PRICE
     --IF PRICE IS ZERO OR NULL, CALCULATE IT USING SALES AND QUANTITY
     --PRICE IS NEGATIVE, CONVERT IT TO A POSITIVE VALUE      <<<<<<<<<<<<<<<<<<<<<<<<<<<<<                            
                                                                                    --    ^ 
                                                                                    --    ^
     --what u did till now : checked if the foreign keys are or not in the common table,  ^
     --checked if the dates are valid,if the rules of sales and prices are correct ^^^^^^^^
    
     --NOW YOU GOTTA RECREATE THE TABLE SINCE NEW DATA TYPES APPEARED

       ---------------------SILVER LAYER-----------------
   
         SELECT * FROM silver.crm_sales_details
    
      --check if any product key from sales details isn't in the prod info, this is the foreign key
      SELECT sls_prd_key FROM silver.crm_sales_details
      WHERE sls_prd_key NOT IN (SELECT prd_key FROM silver.crm_prd_info)
    
      --check if any customer key from sales details isn't in the cust info, this is the foreign key
      SELECT sls_cust_id FROM silver.crm_sales_details
      WHERE sls_cust_id NOT IN (SELECT sls_cust_id FROM silver.crm_cust_info)
    
      --check if the sales are not equal to the quantity * price or if its null or 0
      SELECT * FROM silver.crm_sales_details
      WHERE sls_sales != sls_quantity * sls_price OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_sales IS NULL

---------------------------
--bronze.erp_cust_az12
---------------------------

   ------------------BRONZE LAYER------------------
   
     --check for blank spaces
     SELECT cid FROM bronze.erp_cust_az12
     WHERE cid != TRIM(cid)
    
     --check for duplicates
     SELECT COUNT(*),cid FROM bronze.erp_cust_az12
     GROUP BY cid
     HAVING COUNT(*) > 1 OR cid IS NULL
    
     --check if bdate is valid
     SELECT bdate FROM bronze.erp_cust_az12 WHERE
     LEN(bdate) < 10 OR bdate IS NULL
    
     --out of range date
     SELECT bdate FROM bronze.erp_cust_az12 WHERE
     bdate < '1924-01-01' OR bdate > GETDATE()

    
 -----------------SILVER LAYER--------------------

     --check for blank spaces
     SELECT cid FROM silver.erp_cust_az12
     WHERE cid != TRIM(cid)
    
     --check for duplicates
     SELECT COUNT(*),cid FROM silver.erp_cust_az12
     GROUP BY cid
     HAVING COUNT(*) > 1 OR cid IS NULL
    
     --check if bdate is valid
     SELECT bdate FROM silver.erp_cust_az12 WHERE
     LEN(bdate) < 10 OR bdate IS NULL

--------------------------------------------
--bronze.erp_loc_a101
-------------------------------------------

    -----------BRONZE LAYER---------------
    --check for blank spaces
    SELECT cid FROM bronze.erp_loc_a101
    WHERE cid != TRIM(cid)

    SELECT cntry FROM bronze.erp_loc_a101
    WHERE cntry != TRIM(cntry)

    --check for nulls
    SELECT cid,cntry FROM bronze.erp_loc_a101
    WHERE cid IS NULL OR cntry IS NULL

    --check for consistency
    SELECT DISTINCT cntry FROM bronze.erp_loc_a101
    ORDER BY cntry

    ------------SILVER LAYER-------------

      --check for blank spaces
    SELECT cid FROM silver.erp_loc_a101
    WHERE cid != TRIM(cid)
  
    SELECT cntry FROM silver.erp_loc_a101
    WHERE cntry != TRIM(cntry)
  
    --check for nulls
    SELECT cid,cntry FROM silver.erp_loc_a101
    WHERE cid IS NULL OR cntry IS NULL
  
    --check for consistency
    SELECT DISTINCT cntry FROM silver.erp_loc_a101
    ORDER BY cntry

------------------------------
--bronze.erp_px_cat_g1v2
-----------------------------
 
 -------------BRONZE LAYER--------------
      
 --check for not matching keys
 SELECT id FROM bronze.erp_px_cat_g1v2
 WHERE ID NOT IN (SELECT cat_id FROM silver.crm_prd_info)

 --check for unwanted spaces
 SELECT * FROM bronze.erp_px_cat_g1v2
 WHERE cat != TRIM(cat)


