# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

invoices_df=(spark.read.format("delta").table("bronze_catalog.invoices_db.invoices_raw")
     .filter("invoice_no is not null" and "Quantity > 0")
     .withColumn("load_time", to_utc_timestamp(col("load_time"), "UTC"))
     .selectExpr("InvoiceNo as invoice_no", "StockCode as stock_code", "Description as description",
                           "Quantity as quantity", "to_date(InvoiceDate, 'd-M-y H.m') as invoice_date", 
                           "UnitPrice as unit_price", "CustomerID as customer_id", "Country as country",
                           "year(to_date(InvoiceDate, 'd-M-y H.m')) as invoice_year", 
                           "month(to_date(InvoiceDate, 'd-M-y H.m')) as invoice_month", "load_time"                   
               )
)
(invoices_df.write.format("delta") 
        .mode("append") 
        .saveAsTable("silver_catalog.invoices_db.invoices"))
