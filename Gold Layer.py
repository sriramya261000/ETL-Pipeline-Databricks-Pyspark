# Databricks notebook source
from pyspark.sql import functions as F

def read_customers_data(table_name):
    """
    Read the customers data from a Delta table with a filter for current records.
    """
    return (spark.read.format("delta").table(table_name)
            .filter("is_current = true"))

def read_invoices_data(table_name):
    """
    Read the invoices data from a Delta table.
    """
    return spark.read.format("delta").table(table_name)

def join_invoices_customers(invoices_df, customers_df):
    """
    Join invoices data with customers data on customer_id.
    """
    return (invoices_df.join(customers_df, invoices_df.customer_id == customers_df.customer_id, "left")
            .select(invoices_df["*"], customers_df["customer_name"]))
    
def aggregations(join_df):
    """
    Aggregate the joined data to create a gold layer table

    """
    return (join_df.groupBy("country", "customer_id", "customer_name", "invoice_year", "invoice_month", "invoice_date")
            .agg(
                F.sum(F.expr("quantity * unit_price")).alias("TotalAmount"),
                F.sum("quantity").alias("TotalQuantity"))
            .withColumn("TotalAmount", F.round(F.col("TotalAmount"), 2))
            .orderBy("customer_id", "invoice_year", "invoice_month")
            .select("country", "customer_id", "customer_name", "invoice_year", "invoice_month", "invoice_date", "TotalQuantity", "TotalAmount"))

def write_data_to_table(df, table_name):
    """
    Write the joined data to a Delta table.
    """
    df.write.format("delta").mode("overwrite").saveAsTable(table_name)

# Define table names
customers_table = "silver_catalog.customers_db.customers_scd"
invoices_table = "silver_catalog.invoices_db.invoices"
output_table = "gold_catalog.sales_db.sales"

# Execute the ETL process

customers_df = read_customers_data(customers_table)
invoices_df = read_invoices_data(invoices_table)

joined_df = join_invoices_customers(invoices_df, customers_df)
# display(joined_df)

aggregated_df = aggregations(joined_df)
# display(aggregated_df)

write_data_to_table(aggregated_df, output_table) # write data to gold layer table

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from gold_catalog.sales_db.sales;
