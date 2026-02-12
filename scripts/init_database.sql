/*
Create database and schemas
*/

-- Drop and recreate the 'dwh' database
DROP DATABASE IF EXISTS dwh;

-- Create the 'dwh' database
CREATE DATABASE dwh;

-- Create schemas
CREATE SCHEMA bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold;
