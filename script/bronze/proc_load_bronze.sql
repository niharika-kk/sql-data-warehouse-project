/*
=================================================================
Stored Procedure: load bronze layer (source --> bronze)
=================================================================
Script Purpose:
  This stored procedure loads data into the bronze schema from external csv.
  It performs the following action:
  - truncate bronze table before loading the data.
  - uses 'BULK INSERT' command to load csv data from csv files to bronze.
Parameters:
  None
  This stored procedure does not accept any parameters or return value.
Usage Example:
   EXEC broze.load_bronze
=================================================================
*/
create or alter procedure bronze.load_bronze as
begin
  declare @start_time as datetime,@end_time as datetime;
  print'==========================================================';
  print 'bronze layer loading';
  print'==========================================================';
  
  print'----------------------------------------------------------';
  print 'loading crm tables';
  print'----------------------------------------------------------';

  set @start_time=getdate();
    truncate table bronze.crm_cust_info;
    bulk insert bronze.crm_cust_info from 'C:\temp\data with bara_sql\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
    with(
     FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '0x0a',
        CODEPAGE = '65001',  -- important for UTF-8
        TABLOCK
    );
    set @end_time=GETDATE();
    print '>>load duration: ' +cast(datediff(second,@start_time,@end_time)as nvarchar) + 'seconds';
    print '------------------';
    --SELECT TOP 10 * FROM bronze.crm_cust_info;-- checking data

    set @start_time=GETDATE();
    truncate table bronze.crm_prd_info;
    bulk insert bronze.crm_prd_info from 'C:\temp\data with bara_sql\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
    with(
     FIRSTROW = 2,
        FIELDTERMINATOR = ',',
        ROWTERMINATOR = '0x0a',
        CODEPAGE = '65001',  -- important for UTF-8
        TABLOCK
    );
    set @end_time=getdate();
    print '>>load duration: ' +cast(datediff(second,@start_time,@end_time)as nvarchar) +'seconds';
    print '------------------';
    --SELECT TOP 5 * FROM bronze.crm_prd_info;-- used for checking  the contents

    set @start_time=getdate();
    truncate table bronze.crm_sales_details;
    bulk insert bronze.crm_sales_details from 'C:\temp\data with bara_sql\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
    with(
       firstrow = 2,
       fieldterminator=',',
       tablock
       );
    set @end_time=GETDATE();
    print '>>load duration: ' +cast(datediff(second,@start_time,@end_time)as nvarchar) +'seconds';
    print '------------------';


    print'-----------------------------------------------------';
    print 'loading erp tables';
    print'-----------------------------------------------------';

    set @start_time=GETDATE();
     truncate table bronze.erp_cust_az12;
     bulk insert bronze.erp_cust_az12 from 'C:\temp\data with bara_sql\sql-data-warehouse-project\datasets\source_erp\cust_az12.csv'
       with(
             firstrow=2,
             fieldterminator=',',
             tablock
             );
    set @end_time=GETDATE();
    print '>>load duration: ' +cast(datediff(second,@start_time,@end_time)as nvarchar) +'seconds';
    print '------------------';

    set @start_time=getdate();
    truncate table bronze.erp_loc_a101;
      bulk insert bronze.erp_loc_a101 from 'C:\temp\data with bara_sql\sql-data-warehouse-project\datasets\source_erp\loc_a101.csv'
      with(
          firstrow=2,
          fieldterminator=',',
          tablock
          );
    set @end_time=GETDATE();
    print '>>load duration: ' +cast(datediff(second,@start_time,@end_time)as nvarchar) +'seconds';
    print '------------------';

    set @start_time=getdate();
    truncate table bronze.erp_px_cat_g1v2;
    bulk insert bronze.erp_px_cat_g1v2 from 'C:\temp\data with bara_sql\sql-data-warehouse-project\datasets\source_erp\px_cat_g1v2.csv'
     with(
       firstrow=2,
       fieldterminator=',',
       tablock);
    set @end_time=GETDATE();
    print '>>load duration: ' +cast(datediff(second,@start_time,@end_time)as nvarchar) +'seconds';
    print '------------------';
 end
 EXEC bronze.load_bronze
