# Databricks notebook source
# MAGIC %md
# MAGIC ###### Column Level Masking

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG bronze_catalog;
# MAGIC USE DATABASE customers_db;
# MAGIC
# MAGIC CREATE or replace FUNCTION ssn_mask(ssn STRING)
# MAGIC   RETURN CASE WHEN IS_ACCOUNT_GROUP_MEMBER('admin') THEN ssn ELSE '***-**-****' END;
# MAGIC
# MAGIC ALTER table customers_raw ALTER COLUMN ssn SET MASK SSN_MASK;

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG silver_catalog;
# MAGIC USE DATABASE customers_db;
# MAGIC
# MAGIC CREATE or replace FUNCTION ssn_mask(ssn STRING)
# MAGIC   RETURN CASE WHEN IS_ACCOUNT_GROUP_MEMBER('admin') THEN ssn ELSE '***-**-****' END;
# MAGIC   
# MAGIC ALTER table customers_scd ALTER COLUMN ssn SET MASK SSN_MASK;

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG gold_catalog;
# MAGIC USE DATABASE sales_db;
# MAGIC
# MAGIC CREATE or replace FUNCTION ssn_mask(ssn STRING)
# MAGIC   RETURN CASE WHEN IS_ACCOUNT_GROUP_MEMBER('admin') THEN ssn ELSE '***-**-****' END;
# MAGIC
# MAGIC ALTER table sales ALTER COLUMN ssn SET MASK SSN_MASK;

# COMMAND ----------

# MAGIC %md
# MAGIC ###### Row level access polices

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG gold_catalog;
# MAGIC USE DATABASE sales_db;
# MAGIC
# MAGIC CREATE or  replace FUNCTION us_filter(country STRING)
# MAGIC RETURN IF(IS_ACCOUNT_GROUP_MEMBER('admin'), true, country='United States');
# MAGIC
# MAGIC ALTER TABLE sales SET ROW FILTER us_filter on (country);
