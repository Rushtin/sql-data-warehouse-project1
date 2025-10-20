/*
Stored Procedure: Load Bronze Layer (Source -> Bronze)
Script Purpose: 
  This stored procedure loads data into the 'bronze; schema from external csv files.
  It performs the following action:
    - Truncated the bronze tables before loading data
    - Uses the 'BULK INSERT' command to load data from csv files to bronze tables

No parameters 

Usage example: 
  EXEC bronze.load_bronze;
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS 
	BEGIN
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME
	BEGIN TRY
		PRINT 'LOADING Bronze Layer';

		PRINT '---------------------------------------';
		PRINT 'LOADING CRM Tables';
		SET @start_time = GETDATE();
		PRINT '---------------------------------------';
		PRINT '>> Truncating Table: bronze.crm_cust_info';
		TRUNCATE TABLE bronze.crm_cust_info;
		PRINT 'Inserting Table';
		BULK INSERT bronze.crm_cust_info 
		FROM 'C:\Users\Rushtin\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK 
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR ) + ' seconds';

		PRINT '>> Truncating Table: bronze.crm_prd_info';

		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.crm_prd_info;
		PRINT 'Inserting Table';
		BULK INSERT bronze.crm_cust_info 
		FROM 'C:\Users\Rushtin\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK 
		);
		SET @end_time = GETDATE();

		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR ) + ' seconds';

		PRINT '>> Truncating Table: bronze.crm_sales_details';
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.crm_sales_details;
		PRINT 'Inserting Table';
		BULK INSERT bronze.crm_sales_details 
		FROM 'C:\Users\Rushtin\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK 
		);
		SET @end_time = GETDATE();

		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR ) + ' seconds';

		PRINT '---------------------------------------';
		PRINT 'LOADING ERP Tables';
		PRINT '---------------------------------------';
		PRINT '>> Truncating Table: bronze.erp_cust_az12';
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.erp_cust_az12;
		PRINT 'Inserting Table';
		BULK INSERT bronze.erp_cust_az12 
		FROM 'C:\Users\Rushtin\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK 
		);
		SET @end_time = GETDATE();

		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR ) + ' seconds';

		PRINT '>> Truncating Table: bronze.erp_loc_a101';
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.erp_loc_a101;
		PRINT 'Inserting Table';
		BULK INSERT bronze.erp_loc_a101 
		FROM 'C:\Users\Rushtin\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK 
		);
		SET @end_time = GETDATE();

		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR ) + ' seconds';

		PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
		SET @start_time = GETDATE();
		TRUNCATE TABLE bronze.erp_px_cat_g1v2;
		PRINT 'Inserting Table';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\Rushtin\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW = 2,
			FIELDTERMINATOR = ',',
			TABLOCK 
		);
		SET @end_time = GETDATE();
		PRINT '>> Load Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) as NVARCHAR ) + ' seconds';
		SET @batch_start_time = GETDATE();
		SET @batch_end_time = GETDATE();
		PRINT '==========================================';
		PRINT 'Loading Bronze Layer is Completed';
		PRINT ' -Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds';
		PRINT '==========================================';
	END TRY
	BEGIN CATCH
		PRINT '==================ERROR OCCURED=====================';
		PRINT ERROR_MESSAGE();
		PRINT CAST (ERROR_NUMBER() AS NVARCHAR);
		PRINT CAST (ERROR_STATE() AS NVARCHAR);
	END CATCH
END
