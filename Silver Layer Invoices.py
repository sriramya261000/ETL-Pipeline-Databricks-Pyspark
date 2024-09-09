# Databricks notebook source
# MAGIC %md
# MAGIC ##### 1. Load invoices data from the bronze layer into the silver layer and perform transformations

# COMMAND ----------

from pyspark.sql.functions import col, to_utc_timestamp, to_date, year, month

def read_data(table_name: str):
    """Reads data from a Delta table."""
    return (spark.read.format("delta").table(table_name))

def transform_data(df):
    """Transforms the data."""
    return (df.filter("InvoiceNo is not null and Quantity > 0")
            .withColumn("load_time", to_utc_timestamp(col("load_time"), "UTC"))
            .selectExpr("InvoiceNo as invoice_no", "StockCode as stock_code", "Description as description",
                        "Quantity as quantity", "to_date(InvoiceDate, 'd-M-y H.m') as invoice_date", 
                        "UnitPrice as unit_price", "CustomerID as customer_id", "Country as country",
                        "year(to_date(InvoiceDate, 'd-M-y H.m')) as invoice_year", 
                        "month(to_date(InvoiceDate, 'd-M-y H.m')) as invoice_month", "load_time"))

def write_data(df, table_name: str):
    """Writes data to a Delta table."""
    (df.write.format("delta")
       .mode("append")
       .saveAsTable(table_name))

def main():
    # Define table names
    bronze_table = "bronze_catalog.invoices_db.invoices_raw"
    silver_table = "silver_catalog.invoices_db.invoices"
    
    # Pipeline
    df = read_data(bronze_table)
    df_transformed = transform_data(df)
    write_data(df_transformed, silver_table)

if __name__ == "__main__":
    main()


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver_catalog.invoices_db.invoices
