/*

Script Purpose :

	This script creates a new db called 'DataWareHouse' after checking if it already exists.
	If it does, it will drop it and create a new one.
	There are also 3 schemas created 'bronze','silver','gold'.



*/




USE master;
GO;


IF EXISTS(SELECT 1 FROM sys.databases WHERE name = 'DataWareHouse')

BEGIN	
	ALTER DATABASE DataWareHouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
	DROP DATABASE DataWareHouse;
END;
GO

--create database
CREATE DATABASE DataWareHouse;
GO
USE DataWareHouse;
GO
--#1. create the schemas

CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;

