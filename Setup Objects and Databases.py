# Databricks notebook source
# MAGIC %md
# MAGIC %md
# MAGIC ##### Creating and granting access to objects 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS dev MANAGED LOCATION 'gs://databricks-2944430982339670-unitycatalog/2944430982339670';
# MAGIC CREATE DATABASE IF NOT EXISTS dev.demo_db;
# MAGIC
# MAGIC CREATE EXTERNAL VOLUME IF NOT EXISTS dev.demo_db.landing_zone
# MAGIC LOCATION 'gs://databricks-storage-bucket-external/data_files';
# MAGIC
# MAGIC GRANT ALL PRIVILEGES ON CATALOG dev TO `admin`;
# MAGIC GRANT BROWSE,EXECUTE,READ VOLUME,SELECT,USE CATALOG,USE SCHEMA ON CATALOG dev TO `Dev`;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS bronze_layer MANAGED LOCATION 'gs://databricks-2944430982339670-unitycatalog/2944430982339670';
# MAGIC CREATE DATABASE IF NOT EXISTS bronze_layer.customers_db;
# MAGIC CREATE DATABASE IF NOT EXISTS bronze_layer.invoices_db;
# MAGIC
# MAGIC GRANT ALL PRIVILEGES ON CATALOG bronze_catalog TO `admin`;
# MAGIC GRANT BROWSE,EXECUTE,READ VOLUME,SELECT,USE CATALOG,USE SCHEMA ON CATALOG bronze_catalog TO `Dev`;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS silver_catalog MANAGED LOCATION 'gs://databricks-2944430982339670-unitycatalog/2944430982339670';
# MAGIC CREATE DATABASE IF NOT EXISTS silver_catalog.customers_db;
# MAGIC CREATE DATABASE IF NOT EXISTS silver_catalog.invoices_db;
# MAGIC
# MAGIC GRANT ALL PRIVILEGES ON CATALOG silver_catalog TO `admin`;
# MAGIC GRANT BROWSE,EXECUTE,READ VOLUME,SELECT,USE CATALOG,USE SCHEMA ON CATALOG silver_catalog TO `Dev`;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS gold_catalog MANAGED LOCATION 'gs://databricks-2944430982339670-unitycatalog/2944430982339670';
# MAGIC CREATE DATABASE IF NOT EXISTS gold_catalog.sales_db;
# MAGIC
# MAGIC GRANT ALL PRIVILEGES ON CATALOG gold_catalog TO `admin`;
# MAGIC GRANT BROWSE,EXECUTE,READ VOLUME,SELECT,USE CATALOG,USE SCHEMA ON CATALOG gold_catalog TO `Dev`;

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Changing the ownership to other users

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER CATALOG test OWNER TO `admin`;

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Deleting customers schema

# COMMAND ----------

# MAGIC %fs rm -r '/Volumes/dev/demo_db/landing_zone/customers_schema'

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Deleting invoices schema 

# COMMAND ----------

# MAGIC %fs rm -r '/Volumes/dev/demo_db/landing_zone/invoices_schema'

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Dropping tables from bronze, silver and gold catalogs

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE bronze_catalog.customers_db.customers_raw;
# MAGIC DROP TABLE bronze_catalog.invoices_db.invoices_raw;
# MAGIC
# MAGIC DROP TABLE silver_catalog.customers_db.customers_scd;
# MAGIC DROP TABLE silver_catalog.invoices_db.invoices;
# MAGIC
# MAGIC DROP TABLE gold_catalog.sales_db.sales;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from gold_catalog.sales_db.sales
