/*
-----------------------------------
Stored Procedure: Load Bronze Layer
-----------------------------------
*/

CREATE OR REPLACE PROCEDURE bronze.load_bronze() LANGUAGE plpgsql AS $$
DECLARE
	start_time TIMESTAMP;
	end_time TIMESTAMP;
	batch_start_time TIMESTAMP;
	batch_end_time TIMESTAMP;
BEGIN
	batch_start_time := CURRENT_TIMESTAMP;
	RAISE NOTICE '===========================';
	RAISE NOTICE 'Loading Bronze Layer';	
	RAISE NOTICE '===========================';	
	
	RAISE NOTICE 'Loading CRM tables';
	
	start_time := CURRENT_TIMESTAMP;
	RAISE NOTICE '>> Truncating Table: bronze.crm_cust_info';
	TRUNCATE bronze.crm_cust_info RESTART IDENTITY;

	RAISE NOTICE '>> Inserting Data Into: bronze.crm_cust_info';
	COPY bronze.crm_cust_info FROM 'D:\DE_projects\dwh_project\datasets\source_crm\cust_info.csv' WITH (FORMAT csv, HEADER TRUE, DELIMITER ',');
	end_time := CURRENT_TIMESTAMP;
	RAISE NOTICE 'Load Duration: %', (end_time - start_time);
	
	start_time := CURRENT_TIMESTAMP;
	RAISE NOTICE '>> Truncating Table: bronze.crm_prd_info';
	TRUNCATE bronze.crm_prd_info RESTART IDENTITY;
	
	RAISE NOTICE '>> Inserting Data Into: bronze.crm_prd_info';
	COPY bronze.crm_prd_info FROM 'D:\DE_projects\dwh_project\datasets\source_crm\prd_info.csv' WITH (FORMAT csv, HEADER TRUE, DELIMITER ',');
	end_time := CURRENT_TIMESTAMP;
	RAISE NOTICE 'Load Duration: %', (end_time - start_time);	

	start_time := CURRENT_TIMESTAMP;
	RAISE NOTICE '>> Truncating Table: bronze.crm_sales_details';
	TRUNCATE bronze.crm_sales_details RESTART IDENTITY;

	RAISE NOTICE '>> Inserting Data Into: bronze.crm_sales_details';
	COPY bronze.crm_sales_details FROM 'D:\DE_projects\dwh_project\datasets\source_crm\sales_details.csv' WITH (FORMAT csv, HEADER TRUE, DELIMITER ',');
	end_time := CURRENT_TIMESTAMP;
	RAISE NOTICE 'Load Duration: %', (end_time - start_time);	

	RAISE NOTICE 'Loading ERP tables';	

	start_time := CURRENT_TIMESTAMP;
	RAISE NOTICE '>> Truncating Table: bronze.erp_cust_az12';
	TRUNCATE bronze.erp_cust_az12 RESTART IDENTITY;

	RAISE NOTICE '>> Inserting Data Into: bronze.erp_cust_az12';
	COPY bronze.erp_cust_az12 FROM 'D:\DE_projects\dwh_project\datasets\source_erp\CUST_AZ12.csv' WITH (FORMAT csv, HEADER TRUE, DELIMITER ',');
	end_time := CURRENT_TIMESTAMP;
	RAISE NOTICE 'Load Duration: %', (end_time - start_time);	

	start_time := CURRENT_TIMESTAMP;
	RAISE NOTICE '>> Truncating Table: bronze.erp_loc_a101';
	TRUNCATE bronze.erp_loc_a101 RESTART IDENTITY;
	
	RAISE NOTICE '>> Inserting Data Into: bronze.erp_loc_a101';
	COPY bronze.erp_loc_a101 FROM 'D:\DE_projects\dwh_project\datasets\source_erp\LOC_A101.csv' WITH (FORMAT csv, HEADER TRUE, DELIMITER ',');
	end_time := CURRENT_TIMESTAMP;
	RAISE NOTICE 'Load Duration: %', (end_time - start_time);		

	start_time := CURRENT_TIMESTAMP;
	RAISE NOTICE '>> Truncating Table: bronze.erp_px_cat_g1v2';
	TRUNCATE bronze.erp_px_cat_g1v2 RESTART IDENTITY;

	RAISE NOTICE '>> Inserting Data Into: bronze.erp_px_cat_g1v2';
	COPY bronze.erp_px_cat_g1v2 FROM 'D:\DE_projects\dwh_project\datasets\source_erp\PX_CAT_G1V2.csv' WITH (FORMAT csv, HEADER TRUE, DELIMITER ',');
	end_time := CURRENT_TIMESTAMP;
	RAISE NOTICE 'Load Duration: %', (end_time - start_time);	
	
	batch_end_time := CURRENT_TIMESTAMP;
	RAISE NOTICE 'Loading Bronze Layer is Completed. Total Load Duration: %', (batch_end_time - batch_start_time);

EXCEPTION
	WHEN undefined_table THEN RAISE NOTICE 'Table not found %', SQLERRM;
	WHEN OTHERS THEN RAISE NOTICE 'General error loading bronze layer: % (code %)', SQLERRM, SQLSTATE;

END;
$$;

CALL bronze.load_bronze();

