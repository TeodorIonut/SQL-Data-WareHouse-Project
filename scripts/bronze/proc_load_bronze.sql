/*

THE PROCEDURE IS USED FOR FILLING THE TABLES WITH THE RAW DATA FROM THE .CSV FILES
USED BULK INSERT FOR FILLING THE TABLES
USED SOME CONDITIONS, EACH FIRST ROW STARTS FROM LINE 2, THE DATA IS SEPARATED BY COMMAS, USED TABLOCK FOR PERFORMANCE PURPOSES
SET SOME VARIABLES TO MONITORIZE THE WRITE SPEED OF EACH TABLE
USED A TRY CATCH BLOCK TO RETURN SOME ERRORS IN CASE SOMETHING GOES WRONG WHILE FILLING TABLES

*/



CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN TRY



DECLARE @start_time DATETIME, @end_time DATETIME,@batch_start_time DATETIME,@batch_end_time DATETIME;

    SET @batch_start_time = GETDATE()

    PRINT('============================================')
    PRINT('Loading bronze layer')
    PRINT('============================================')
    PRINT('------------------------------------')
    PRINT('Loading CRM Tables')


    SET @start_time = GETDATE();
    TRUNCATE TABLE bronze.crm_cust_info;

    BULK INSERT bronze.crm_cust_info
    FROM 'C:\Users\POPIT\Desktop\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
    WITH ( 
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        TABLOCK
    );

    SET @end_time = GETDATE();
    PRINT('LOAD DURATION: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR));

    SELECT COUNT(*) FROM bronze.crm_cust_info;

    SET @start_time = GETDATE();
    TRUNCATE TABLE bronze.crm_prd_info;

    BULK INSERT bronze.crm_prd_info
    FROM 'C:\Users\POPIT\Desktop\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
    WITH ( 
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        TABLOCK
    );
    SET @end_time = GETDATE();
    PRINT('LOAD DURATION: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR));

    SET @start_time = GETDATE();

    TRUNCATE TABLE bronze.crm_sales_details;

    BULK INSERT bronze.crm_sales_details
    FROM 'C:\Users\POPIT\Desktop\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
    WITH ( 
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        TABLOCK
    );

    SET @end_time = GETDATE();
    PRINT('LOAD DURATION: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR));

    SET @start_time = GETDATE()
    TRUNCATE TABLE bronze.erp_cust_az12;

    BULK INSERT bronze.erp_cust_az12
    FROM 'C:\Users\POPIT\Desktop\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
    WITH ( 
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        TABLOCK
    );

    SET @end_time = GETDATE();
    PRINT('LOAD DURATION: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR));

    PRINT('------------------------------------')
    PRINT('Loading ERP Tables')

    SET @start_time = GETDATE()
    TRUNCATE TABLE bronze.erp_loc_a101;

    BULK INSERT bronze.erp_loc_a101
    FROM 'C:\Users\POPIT\Desktop\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
    WITH ( 
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        TABLOCK
    );
    SET @end_time = GETDATE();
    PRINT('LOAD DURATION: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR));

    SET @start_time = GETDATE();
    TRUNCATE TABLE bronze.erp_px_cat_g1v2;

    BULK INSERT bronze.erp_px_cat_g1v2
    FROM 'C:\Users\POPIT\Desktop\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
    WITH ( 
        FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        TABLOCK
    );
    SET @end_time = GETDATE();
    PRINT('LOAD DURATION: ' + CAST(DATEDIFF(second,@start_time,@end_time) AS NVARCHAR));

    SET @batch_end_time = GETDATE()
    PRINT('The procedure loaded in ' + CAST(DATEDIFF(second,@batch_start_time,@batch_end_time) AS NVARCHAR))    
END TRY
BEGIN CATCH
    PRINT('===============================')
    PRINT('ERROR OCCURRED DURING LOADING LAYERS')
    PRINT('ERROR MESSAGE: ' + ERROR_MESSAGE())
    PRINT('ERROR NUMBER: ' + CAST(ERROR_NUMBER() AS NVARCHAR))
    PRINT('===============================')
END CATCH;
