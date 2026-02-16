CREATE OR REPLACE PROCEDURE silver.load_silver() LANGUAGE plpgsql AS $$
DECLARE
	start_time TIMESTAMP;
	end_time TIMESTAMP;
	batch_start_time TIMESTAMP;
	batch_end_time TIMESTAMP;
begin
	batch_start_time := CURRENT_TIMESTAMP;
	RAISE NOTICE '===========================';
	RAISE NOTICE 'Loading Silver Layer';	
	RAISE NOTICE '===========================';

	RAISE NOTICE 'Loading CRM tables';
	
	--Loading crm_cust_info
	start_time := CURRENT_TIMESTAMP;
	RAISE NOTICE '>> Truncating Table: silver.crm_cust_info';
	TRUNCATE silver.crm_cust_info RESTART IDENTITY;
	RAISE NOTICE '>> Inserting Data Into: silver.crm_cust_info';
	insert into silver.crm_cust_info (cst_id, cst_key, cst_firstname, cst_lastname, cst_marital_status, cst_gndr, cst_create_date)
	select 
		cst_id, 
		cst_key, 
		TRIM(cst_firstname) as cst_firstname, 
		TRIM(cst_lastname) as cst_lastname, 
		case 
			when UPPER(TRIM(cst_marital_status)) = 'S' then 'Single'
			when UPPER(TRIM(cst_marital_status)) = 'M' then 'Married'
			else 'n/a'
		end as cst_marital_status,
		case 
			when UPPER(TRIM(cst_gndr)) = 'F' then 'Female'
			when UPPER(TRIM(cst_gndr)) = 'M' then 'Male'
			else 'n/a'
		end as cst_gndr,
		cst_create_date 
	from (select *, row_number() over(partition by cst_id order by cst_create_date desc) as flag_last
	  from bronze.crm_cust_info
	  where cst_id is not null)
	where flag_last = 1;
	end_time := CURRENT_TIMESTAMP;
	RAISE NOTICE 'Load Duration: %', (end_time - start_time);
	
	--Loading crm_prd_info
	start_time := CURRENT_TIMESTAMP;
	RAISE NOTICE '>> Truncating Table: silver.crm_prd_info';
	TRUNCATE silver.crm_prd_info RESTART IDENTITY;
	RAISE NOTICE '>> Inserting Data Into: silver.crm_prd_info';
	insert into silver.crm_prd_info (prd_id, cat_id, prd_key, prd_nm, prd_cost, prd_line, prd_start_dt, prd_end_dt)
	select 
		prd_id,
		replace(substring(prd_key, 1, 5), '-', '_') as cat_id,
		substring(prd_key, 7, length(prd_key)) as prd_key,
		prd_nm,
		coalesce(prd_cost, 0) as prd_cost,
		case
			when UPPER(TRIM(prd_line)) = 'M' then 'Mountain'
			when UPPER(TRIM(prd_line)) = 'R' then 'Road'
			when UPPER(TRIM(prd_line)) = 'S' then 'Other Sales'
			when UPPER(TRIM(prd_line)) = 'T' then 'Touring'
			else 'n/a'
		end as prd_line,
		prd_start_dt,
		lead(prd_start_dt) over(partition by prd_key order by prd_start_dt asc) -1 as prd_end_dt
	from bronze.crm_prd_info;
	end_time := CURRENT_TIMESTAMP;
	RAISE NOTICE 'Load Duration: %', (end_time - start_time);
	
	--Loading crm_sales_details
	start_time := CURRENT_TIMESTAMP;
	RAISE NOTICE '>> Truncating Table: silver.crm_sales_details';
	TRUNCATE silver.crm_sales_details RESTART IDENTITY;
	RAISE NOTICE '>> Inserting Data Into: silver.crm_sales_details';
	insert into silver.crm_sales_details (sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt, sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price)
	select 
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		case
			when sls_order_dt = 0 or length(sls_order_dt::VARCHAR) != 8 then null
			else (sls_order_dt::VARCHAR)::DATE
		end as sls_order_dt,
		case
			when sls_ship_dt = 0 or length(sls_ship_dt::VARCHAR) != 8 then null
			else (sls_ship_dt::VARCHAR)::DATE
		end as sls_ship_dt,
		case
			when sls_due_dt = 0 or length(sls_due_dt::VARCHAR) != 8 then null
			else (sls_due_dt::VARCHAR)::DATE
		end as sls_due_dt,
		case
			when sls_sales is null or sls_sales <=0 or sls_sales != ABS(sls_price) * sls_quantity then ABS(sls_price) * sls_quantity
			else sls_sales
		end as sls_sales, 
		sls_quantity,
		case
			when sls_price is null or sls_price <0 then sls_sales / nullif(sls_quantity, 0)
			when sls_price < 0 then ABS(sls_price)
			else sls_price
		end as sls_price
	from bronze.crm_sales_details;
	end_time := CURRENT_TIMESTAMP;
	RAISE NOTICE 'Load Duration: %', (end_time - start_time);
	
	--Loading erp_cust_az12
	start_time := CURRENT_TIMESTAMP;
	RAISE NOTICE '>> Truncating Table: silver.erp_cust_az12';
	TRUNCATE silver.erp_cust_az12 RESTART IDENTITY;
	RAISE NOTICE '>> Inserting Data Into: silver.erp_cust_az12';
	insert into silver.erp_cust_az12 (cid, bdate, gen)
	select 
		case 
			when cid like 'NAS%' then SUBSTRING(cid, 4, length(cid))
			else cid
		end as cid,
		case
			when bdate > current_timestamp then null
			else bdate
		end as bdate,
		case 
			when TRIM(gen) = '' or gen is null then 'n/a'
			when TRIM(gen) = 'F' then 'Female'
			when TRIM(gen) = 'M' then 'Male'
			else TRIM(gen)
		end as gen
	from bronze.erp_cust_az12;
	end_time := CURRENT_TIMESTAMP;
	RAISE NOTICE 'Load Duration: %', (end_time - start_time);
	
	--Loading erp_loc_a101
	start_time := CURRENT_TIMESTAMP;
	RAISE NOTICE '>> Truncating Table: silver.erp_loc_a101';
	TRUNCATE silver.erp_loc_a101 RESTART IDENTITY;
	RAISE NOTICE '>> Inserting Data Into: silver.erp_loc_a101';
	insert into silver.erp_loc_a101 (cid, cntry)
	select 
		REPLACE(TRIM(cid), '-', '') as cid,
		case 
			when TRIM(cntry) = '' or cntry is null then 'n/a'
			when TRIM(cntry) IN ('US', 'USA') then 'United States'
			when TRIM(cntry) = 'DE' then 'Germany'
			else TRIM(cntry)
		end as cntry
	from bronze.erp_loc_a101;
	end_time := CURRENT_TIMESTAMP;
	RAISE NOTICE 'Load Duration: %', (end_time - start_time);
	
	--Loading erp_px_cat_g1v2
	start_time := CURRENT_TIMESTAMP;
	RAISE NOTICE '>> Truncating Table: silver.erp_px_cat_g1v2';
	TRUNCATE silver.erp_px_cat_g1v2 RESTART IDENTITY;
	RAISE NOTICE '>> Inserting Data Into: silver.erp_px_cat_g1v2';
	insert into silver.erp_px_cat_g1v2 (id, cat, subcat, maintenance)
	select * 
	from bronze.erp_px_cat_g1v2;

	batch_end_time := CURRENT_TIMESTAMP;
	RAISE NOTICE 'Loading Silver Layer is Completed. Total Load Duration: %', (batch_end_time - batch_start_time);

EXCEPTION
	WHEN undefined_table THEN RAISE NOTICE 'Table not found %', SQLERRM;
	WHEN OTHERS THEN RAISE NOTICE 'General error loading silver layer: % (code %)', SQLERRM, SQLSTATE;
end;
$$;

CALL silver.load_silver();
