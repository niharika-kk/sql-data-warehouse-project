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
