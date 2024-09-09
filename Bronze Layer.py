# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------


def data_ingestion(input_path: str, checkpoint_location: str, table_name: str):

    # Define the source DataFrame
    source_df = (spark.readStream
                 .format("cloudFiles")
                 .option("cloudFiles.format", "csv")
                 .option("header", "true")
                 .option("cloudFiles.inferColumnTypes", "true")
                 .option("cloudFiles.schemaLocation", checkpoint_location)
                 .load(input_path)
                 .withColumn("load_time", current_timestamp())
    )

    # Define the write query
    write_query = (source_df.writeStream
                   .format("delta")
                   .option("checkpointLocation", checkpoint_location)
                   .option("mergeSchema", "true")
                   .outputMode("append")
                   .trigger(availableNow=True)
                   .toTable(table_name)
    )

    return write_query

# Parameters for customers data
customers_input_path = '/Volumes/dev/demo_db/landing_zone/customers'
customers_checkpoint_location = '/Volumes/dev/demo_db/landing_zone/customers_schema'
customers_table_name = 'bronze_catalog.customers_db.customers_raw'

# Parameters for invoices data
invoices_input_path = '/Volumes/dev/demo_db/landing_zone/invoices'
invoices_checkpoint_location = '/Volumes/dev/demo_db/landing_zone/invoices_schema'
invoices_table_name = 'bronze_catalog.invoices_db.invoices_raw'

# Process customers data
customers_query = data_ingestion(customers_input_path, customers_checkpoint_location, customers_table_name)

print(customers_query)

# # Process invoices data
invoices_query = data_ingestion(invoices_input_path, invoices_checkpoint_location, invoices_table_name)

print(invoices_query)

# .option("cloudFiles.schemaHints", "InvoiceNo string, CustomerID string")
