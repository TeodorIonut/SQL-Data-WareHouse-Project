
/*

Here lies the procedure for the silver layer, checking for bad data,creating the tables, and inserting into them te filtered data

*/


CREATE OR ALTER PROCEDURE silver.load_silver AS

BEGIN

    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
    BEGIN TRY
        SET @batch_start_time = GETDATE();

    --BUILDING SILVER LAYER

    --ANALYSING: EXPLORE & UNDERSTAND THE DATA 
    --CODING : DATA CLEANSE -- CHECK QUALITY OF BRONZE -> WRITE DATA TRANSFORMATIONS -> INSERT INTO SILVER
    --VALIDATING : DATA CORRECTNESS CHECK
    --DATA DOCUMENTING VERSIONING IN GIT



    --METADATA COLUMNS : EXTRA COLUMNS ADDED BY DATA ENGINEERS THAT DO NOT ORIGINATE FROM THE SOURCE DATA

    --create_date : The record's load timestamp
    --update_date : The record's last update timestamp
    --source_system : The origin system of the record
    --file_location : the file source of record


    --BASICALLY IN SILVER LAYER CHECK IF U GOT BLANK SPACES, MISSING DATA LIKE NULLS OR DUPLICATED KEYS
    --LIKE IDS


    --crm.cust.info 
    

    PRINT('=======================================')
    PRINT('Inserting into silver.crm_cust_info table')
    INSERT INTO silver.crm_cust_info(cst_id,cst_key,cst_firstname,cst_lastname,cst_material_status,cst_gndr,cst_create_date)
    SELECT 
    cst_id,
    cst_key,
    TRIM(cst_firstname) AS cst_firstname,--remove unwanted characters with trim
    TRIM(cst_lastname) AS cst_lastname,
    CASE WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single' --data normalization (renames values into a more friendly to understand manner)
    WHEN UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
    WHEN UPPER(TRIM(cst_material_status)) IS NULL THEN 'UNKNOWN' END AS cst_marital_status, -- handling missing data by replacing nulls with a value
    CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
    WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'MALE'
    WHEN UPPER(TRIM(cst_gndr)) IS NULL THEN 'UNKNOWN' END AS cst_gndr,
    cst_create_date
    FROM (
            SELECT
            *,
            ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
            FROM bronze.crm_cust_info
            WHERE cst_id IS NOT NULL
            )t WHERE flag_last = 1 -- removing duplicates


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

     PRINT('=======================================')
     PRINT('Creating the silver.crm_prd_info table')
     IF OBJECT_ID ('silver.crm_prd_info', 'U') IS NOT NULL
        DROP TABLE silver.crm_prd_info;

        CREATE TABLE silver.crm_prd_info (
        prd_id INT,
        cat_id NVARCHAR(50),
        prd_key NVARCHAR(50),
        prd_nm NVARCHAR(50),
        prd_cost INT,
        prd_line NVARCHAR(50),
        prd_start_dt DATE,
        prd_end_dt DATE,
        dwh_create_date DATETIME2 DEFAULT GETDATE()
        );


    
     PRINT('======================================')
     PRINT('Inserting into silver.crm_prd_info')
     INSERT INTO silver.crm_prd_info(
     prd_id,
     cat_id,
     prd_key,
     prd_nm,
     prd_cost,
     prd_line,
     prd_start_dt,
     prd_end_dt)
     SELECT
     prd_id,
     REPLACE(SUBSTRING(prd_key,1,5), '-', '_') AS cat_id, -- column needed to join with id from erp_px
     SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key, -- needed for sales_details
     prd_nm,
     ISNULL(prd_cost,0) AS prd_cost,
     CASE UPPER(TRIM(prd_line))
          WHEN 'M' THEN 'Mountain'
          WHEN  'R' THEN 'Road'
          WHEN  'S' THEN 'other Sales'
          WHEN  'T' THEN 'Touring' 
          ELSE 'n/a' 
          END prd_line,
          CAST(prd_start_dt AS DATE) start_date,
     CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt
     FROM bronze.crm_prd_info


     SELECT * FROM bronze.crm_sales_details

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




    -- todays summary : #1.check for blank spaces, null values, replace ambigous values with friendly to understand values
    -- check for duplicates, 
                        --#2. fix the bad values using whatever, trim,cases,substring,where clauses
                        --#3. create the tables
                        --#4. insert those values into table




    --20.11.2025

    --After inserting filtered data into silver layer do a quality data check

    SELECT
    prd_id,
    COUNT(*)
    FROM silver.crm_prd_info
    GROUP BY prd_id
    HAVING COUNT(*) > 1 OR prd_id IS NULL

    SELECT prd_nm
    FROM silver.crm_prd_info
    WHERE prd_nm != TRIM(prd_nm)

    SELECT prd_cost
    FROM silver.crm_prd_info
    WHERE prd_cost < 0 OR prd_cost IS NULL

    SELECT * FROM
    silver.crm_prd_info
    WHERE prd_end_dt < prd_start_dt


    SELECT * FROM silver.crm_prd_info

    --DERIVED COLUMNS : CREATE NEW COLUMNS BASED ON CALCULATIONS OR TRANSFORMATIONS OF EXISTING ONES

    --DATA ENRICHMENT: ADD NEW, RELEVANT DATA TO ENHANCE THE DATASET FOR ANALYSIS

    --
    --crm_sales_details
     PRINT('======================================')
     PRINT('Inserting into silver.crm_sales_details')
    INSERT INTO silver.crm_sales_details(sls_ord_num,sls_prd_key,sls_cust_id,sls_order_dt,sls_ship_dt,sls_due_dt,sls_sales,sls_quantity,sls_price)
    SELECT
    sls_ord_num,
    sls_prod_key,
    sls_cust_id,
    CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
    ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE) END sls_order_dt, -- can't cast directly from int to date, gotta cast to varchar first then to date
    CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
    ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE) END sls_ship_dt,
    CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
    ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE) END sls_due_dt,
    CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
    THEN sls_quantity * ABS(sls_price) ELSE sls_sales END AS sls_sales,
    sls_quantity,
    CASE WHEN sls_price IS NULL OR sls_price <= 0 THEN sls_sales / NULLIF(sls_quantity,0) ELSE sls_price  END sls_price
    FROM bronze.crm_sales_details




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
     PRINT('======================================')
     PRINT('Creating table silver.crm_sales_details')
    IF OBJECT_ID('silver.crm_sales_details', 'U') IS NOT NULL
    DROP TABLE silver.crm_sales_details

    CREATE TABLE silver.crm_sales_details (
    sls_ord_num NVARCHAR(50),
    sls_prd_key NVARCHAR(50),
    sls_cust_id INT,
    sls_order_dt DATE,
    sls_ship_dt DATE,
    sls_due_dt DATE,
    sls_sales INT,
    sls_quantity INT,
    sls_price INT,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
    )

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




    ---
    --bronze.erp_cust_az12
    SELECT * FROM bronze.erp_cust_az12
    SELECT * FROM silver.crm_cust_info
    PRINT('======================================')
    PRINT('Inserting into table silver.erp_cust_az12')
    TRUNCATE TABLE silver.erp_cust_az12
    INSERT INTO silver.erp_cust_az12(cid,gen,bdate)
    SELECT 
    SUBSTRING(cid,4,LEN(cid)) cid , --retrieving only the key found in the crm_cust_info table
    CASE WHEN gen = 'F' THEN 'Female' --checking for ambigous data
    WHEN gen = 'M' THEN 'Male'
    WHEN gen IS NULL OR gen = '' THEN 'UNKNOWN'
    else gen END gen,
    CASE WHEN bdate > GETDATE() THEN NULL else bdate END bdate
    FROM bronze.erp_cust_az12


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


    PRINT('======================================')
    PRINT('Creating table silver.erp_cust_az12')
    IF OBJECT_ID('silver.erp_cust_az12', 'U') IS NOT NULL
    DROP TABLE silver.erp_cust_az12

    CREATE TABLE silver.erp_cust_az12 (

    cid NVARCHAR(50),
    gen NVARCHAR(50),
    bdate DATE,
    dwh_create_date DATETIME2 DEFAULT GETDATE()
    )

    SELECT * FROM silver.erp_cust_az12

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


    --erp_loc_a101------------------------------------
    SELECT * FROM bronze.erp_loc_a101

    --check for blank spaces
    SELECT cid FROM bronze.erp_loc_a101
    WHERE cid != TRIM(cid)

    SELECT cntry FROM bronze.erp_loc_a101
    WHERE cntry != TRIM(cntry)

    --check for nulls
    SELECT cid,cntry FROM silver.erp_loc_a101
    WHERE cid IS NULL OR cntry IS NULL

    --check for consistency
    SELECT DISTINCT cntry FROM bronze.erp_loc_a101
    ORDER BY cntry

    PRINT('======================================')
    PRINT('Inserting into table silver.erp_loc_a101')
    TRUNCATE TABLE silver.erp_loc_a101;
    INSERT silver.erp_loc_a101 (cid, cntry)
    SELECT
        REPLACE(cid, '-', '') AS cid,
        CASE WHEN cntry IS NULL OR cntry = '' THEN COALESCE(cntry,'UNKNOWN')
        WHEN cntry IN ('USA','US') THEN 'United States'
        WHEN cntry = 'DE' THEN 'Germany'
        ELSE cntry END cntry
    FROM bronze.erp_loc_a101;

    SELECT * FROM silver.erp_loc_a101
    WHERE cntry = 'Unknown'

    PRINT('======================================')
    PRINT('Creating table silver.erp_loc_a101')
    IF OBJECT_ID('silver.erp_loc_a101','U') IS NOT NULL
    DROP TABLE silver.erp_loc_a101

    CREATE TABLE silver.erp_loc_a101 (

    cid NVARCHAR(50),
    cntry NVARCHAR(50),
    dwh_create_date DATETIME2 DEFAULT GETDATE()
    )

    --test to see if the tables can now communicate (cust info and loc a101)
    SELECT cst_firstname,cntry FROM silver.crm_cust_info scp
    INNER JOIN silver.erp_loc_a101 sel ON scp.cst_key = sel.cid

    --erp_px_cat_g1v2-----------------------
    SELECT * FROM bronze.erp_px_cat_g1v2
    SELECT * FROM  silver.crm_prd_info

    TRUNCATE TABLE silver.erp_px_cat_g1v2
    INSERT silver.erp_px_cat_g1v2(id,cat,subcat,maintenance)
    SELECT 
    id,
    cat,
    subcat,
    maintenance
    FROM bronze.erp_px_cat_g1v2


    --check for not matching keys
    SELECT id FROM bronze.erp_px_cat_g1v2
    WHERE ID NOT IN (SELECT cat_id FROM silver.crm_prd_info)

    --check for unwanted spaces
    SELECT * FROM bronze.erp_px_cat_g1v2
    WHERE cat != TRIM(cat)

    --Data standardization

    SELECT DISTINCT cat 
    FROM bronze.erp_px_cat_g1v2


    SELECT * FROM silver.erp_px_cat_g1v2

    SET @batch_end_time = GETDATE();

    END TRY 
   BEGIN CATCH
   PRINT('ERROR OCCURED DURING LOADING BRONZE LAYER')
   PRINT('ERROR' + ERROR_MESSAGE());
   PRINT('ERROR' + CAST(ERROR_NUMBER() AS NVARCHAR));
   PRINT('ERROR' + CAST(ERROR_STATE() AS NVARCHAR));
    END CATCH

 END
