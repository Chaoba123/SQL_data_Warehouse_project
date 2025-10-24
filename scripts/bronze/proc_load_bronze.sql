/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from CSV files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze as
BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '====================================';
		PRINT 'Loading Bronze Layer';
		PRINT '====================================';

		PRINT '------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table : bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;  --- MAKING TABLE EMPTY TO AVOID MULTIPLE LOAD
	
		PRINT '>> Inserting data into: bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\chaob\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.CSV'
		WITH (
			FIRSTROW = 2,  -- INSERT START FROM 2ND ROW
			FIELDTERMINATOR = ',',   -- DELIMITER OF THE FILE
			TABLOCK  -- LOCK TABLE DURING INSERT
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + cast(DATEDIFF(SECOND, @start_time, @end_time ) as NVARCHAR) + ' seconds';
		PRINT '>> ------------------';

		-- SELECT * FROM bronze.crm_cust_info;
		-- SELECT COUNT(*) FROM bronze.crm_cust_info;  -- CHECK THE FILE COUNT ON SHEET

		SET @start_time = GETDATE()
		PRINT '>> Truncating Table : bronze.crm_prd_info';
		TRUNCATE TABLE bronze.crm_prd_info;

		PRINT '>> Inserting data into: bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\chaob\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.CSV'
		WITH (
			FIRSTROW = 2,  -- INSERT START FROM 2ND ROW
			FIELDTERMINATOR = ',',   -- DELIMITER OF THE FILE
			TABLOCK  -- LOCK TABLE DURING INSERT
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration' + cast(DATEDIFF(SECOND, @start_time, @end_time ) as NVARCHAR) + ' seconds';
		PRINT '>> ------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table : bronze.crm_sales_details';
		TRUNCATE TABLE bronze.crm_sales_details;

		PRINT '>> Inserting data into: bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\chaob\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.CSV'
		WITH (
			FIRSTROW = 2,  -- INSERT START FROM 2ND ROW
			FIELDTERMINATOR = ',',   -- DELIMITER OF THE FILE
			TABLOCK  -- LOCK TABLE DURING INSERT
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + cast(DATEDIFF(SECOND, @start_time, @end_time ) as NVARCHAR) + ' seconds';
		PRINT '>> ------------------';

		PRINT '------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '------------------------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table : bronze.erp_loc_a101';
		TRUNCATE TABLE bronze.erp_loc_a101;

		PRINT '>> Inserting data into: bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\chaob\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.CSV'
		WITH (
			FIRSTROW = 2,  -- INSERT START FROM 2ND ROW
			FIELDTERMINATOR = ',',   -- DELIMITER OF THE FILE
			TABLOCK  -- LOCK TABLE DURING INSERT
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + cast(DATEDIFF(SECOND, @start_time, @end_time ) as NVARCHAR) + ' seconds';
		PRINT '>> ------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table : bronze.erp_cust_az12';
		TRUNCATE TABLE bronze.erp_cust_az12

		PRINT '>> Inserting data into: bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\chaob\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.CSV'
		WITH (
			FIRSTROW = 2,  -- INSERT START FROM 2ND ROW
			FIELDTERMINATOR = ',',   -- DELIMITER OF THE FILE
			TABLOCK  -- LOCK TABLE DURING INSERT
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + cast(DATEDIFF(SECOND, @start_time, @end_time ) as NVARCHAR) + ' seconds';
		PRINT '>> ------------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table : bronze.erp_px_cat_g1v2';
		TRUNCATE TABLE bronze.erp_px_cat_g1v2

		PRINT '>> Inserting data into: bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\chaob\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.CSV'
		WITH (
			FIRSTROW = 2,  -- INSERT START FROM 2ND ROW
			FIELDTERMINATOR = ',',   -- DELIMITER OF THE FILE
			TABLOCK  -- LOCK TABLE DURING INSERT
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + cast(DATEDIFF(SECOND, @start_time, @end_time ) as NVARCHAR) + ' seconds';
		PRINT '>> ------------------';

		SET @batch_end_time = GETDATE();
		PRINT '=====================================================';
		PRINT 'Loading Bronze Layer is Completed';
		PRINT 'Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) as NVARCHAR) +' Seconds';
		PRINT '=====================================================';

	END TRY

	BEGIN CATCH
		PRINT '=====================================================';
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT 'ERROR MESSAGE' + ERROR_MESSAGE();
		PRINT 'ERROR MESSAGE' +CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'ERROR MESSAGE' +CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'ERROR MESSAGE' +CAST(ERROR_STATE() AS NVARCHAR);
		PRINT '====================================================='
	END CATCH
END;
