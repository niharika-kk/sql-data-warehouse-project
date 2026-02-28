/*
=====================================================================
CREATE DATABASE AND SCHEMAS
=====================================================================
Script Purpose
    this script creates new database name"datawarehouse" .Additionaly the script sets up three schemas within the databade "bronze","silver","gold".
*/
-----create database master-----
use master;
create database datawarehouse;
use datawarehouse;
---creating the schemas----
CREATE SCHEMA bronze;
go
CREATE SCHEMA silver;
go
CREATE SCHEMA gold;
if object_id('bronze.crm_cust_info','u') is not null drop table bronze.crm_cust_info;
create table bronze.crm_cust_info (
cst_id int,
cst_key nvarchar(50),
cst_firstname nvarchar(50),
cst_lastname nvarchar(50),
cst_material_status nvarchar(50),
cst_gndr nvarchar(50),
cst_create_date date
);
if object_id('bronze.crm_prd_info','u') is not null drop table bronze.crm_prd_info;
create table bronze.crm_prd_info(
prd_id int,
prd_key nvarchar(50),
prd_nm nvarchar(50),
prd_cost int,
prd_line nvarchar(50),
prd_start_st datetime,
prd_end_dt datetime
);
if object_id('bronze.crm_sales_details','u') is not null drop table bronze.crm_sales_details;
create table bronze.crm_sales_details(
sls_ord_num nvarchar(50),
sls_prd_key nvarchar(50),
sls_cust_id int,
sls_order_dt int,
sls_ship_dt int,
als_due_dt int,
sls_sales int,
sls_quantity int,
sls_price int
);
if object_id('bronze.erp_loc_a101','u') is not null drop table bronze.erp_loc_a101;
create table bronze.erp_loc_a101(
cid nvarchar(50),
cntry nvarchar(50)
);
if object_id('bronze.erp_cust_az12','u') is not null drop table bronze.erp_cust_az12 ;
create table bronze.erp_cust_az12(
cid nvarchar(50),
bdate date,
gen nvarchar(50)
);
if object_id('bronze.erp_px_cat_g1v2','u') is not null drop table bronze.erp_px_cat_g1v2;
create table bronze.erp_px_cat_g1v2(
id nvarchar(50),
cat nvarchar(50),
subcat nvarchar(50), 
maintenance nvarchar(50)
);


bulk insert bronze.crm_cust_info from 'C:\temp\data with bara_sql\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
with(
 FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0a',
    CODEPAGE = '65001',  -- important for UTF-8
    TABLOCK
);
ALTER TABLE bronze.crm_cust_info
ALTER COLUMN cst_create_date NVARCHAR(50);
SELECT TOP 10 * 
FROM bronze.crm_cust_info;
select count(*) from bronze.crm_cust_info
