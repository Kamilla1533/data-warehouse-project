-- Check for duplicates and NULL values
select sls_cust_id, count(*) --sls_cust_id, 
from bronze.crm_sales_details
group by sls_cust_id
having count(*) > 1 or sls_cust_id is null;


-- Check for unwanted spaces
select sls_prd_key --sls_ord_num, sls_prd_key
from silver.crm_sales_details
where sls_prd_key != trim(sls_prd_key);

-- Check for NULLs or negative numbers
select * --, sls_order_dt, sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price
from silver.crm_sales_details
where sls_order_dt < 0 or sls_order_dt is null;

--Check for dates columns
select *
from bronze.crm_sales_details
where sls_due_dt < sls_order_dt;

--check for invalid dates
select nullif(sls_order_dt, 0) as sls_order_dt 
from bronze.crm_sales_details
where sls_order_dt = 0;

--check for business rule about sales
select 
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
from bronze.crm_sales_details
where sls_sales != sls_price * sls_quantity or sls_sales is null or sls_quantity is null or sls_price is null
or sls_sales <= 0 or sls_quantity <=0  or sls_price <= 0;

select *
from bronze.crm_sales_details
where sls_cust_id not in (select cst_id from silver.crm_cust_info);

--erp_loc_a101
select 
	case 
		when TRIM(cntry) = '' or cntry is null then 'n/a'
		when TRIM(cntry) IN ('US', 'USA') then 'United States'
		when TRIM(cntry) = 'DE' then 'Germany'
		else TRIM(cntry)
	end as cntry
from bronze.erp_loc_a101;

--checking for quality
SELECT *
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON c.customer_key=f.customer_key
LEFT JOIN gold.dim_products p
ON p.product_key=f.product_key
WHERE p.product_key IS NULL;

select distinct cntry from bronze.erp_loc_a101;
