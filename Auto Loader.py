# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

# MAGIC %md
# MAGIC #####1. Create your bronze layer tables ingesting from the landing zone

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

# COMMAND ----------

# MAGIC %md
# MAGIC #####2. Create your silver layer tables reading data from bronze layer

# COMMAND ----------

from delta.tables import DeltaTable

timestamp_format = "yyyy-MM-dd HH:mm:ss.SSS"
dummy_timestamp = "9999-12-31 23:59:59"
tables_list=[]
tables=spark.catalog.listTables("silver_catalog.customers_db")
for table in tables:
    tables_list.append(table.name)
print(tables_list)
if "customers_scd" not in tables_list:
    df = (spark.read.format("delta").table("bronze_catalog.customers_db.customers_raw")
               .filter("CustomerID is not null")
               .dropDuplicates()
               .withColumn("load_time", to_utc_timestamp(col("load_time"), "UTC"))
               .selectExpr("CustomerID as customer_id", "CustomerName as customer_name", "load_time")
                .withColumn("effective_date", date_format(current_timestamp(), timestamp_format))
                .withColumn("expiration_date", date_format(lit(dummy_timestamp), timestamp_format))
                .withColumn("is_current", lit(True))
        )
    display(df)
    df.write.format("delta").saveAsTable("silver_catalog.customers_db.customers_scd")
else:
    print("Not initial load")
    targetTable = DeltaTable.forName(spark, "silver_catalog.customers_db.customers_scd")
    targetDF=targetTable.toDF()
#     display(targetDF)

    sourceDF = (spark.read.format("delta").table("bronze_catalog.customers_db.customers_raw")
                .filter("CustomerID is not null")
               .dropDuplicates()
               .withColumn("load_time", to_utc_timestamp(col("load_time"), "UTC"))
               .selectExpr("CustomerID as customer_id", "CustomerName as customer_name", "load_time"))
#     display(sourceDF)

    joinDF = sourceDF.join(targetDF, (sourceDF.customer_id == targetDF.customer_id) & (targetDF.is_current == True), "left")\
    .select(sourceDF["*"],\
        targetDF.customer_id.alias("target_customer_id"),\
            targetDF.customer_name.alias("target_customer_name"))
#     display(joinDF)

    filterDF = joinDF.filter(xxhash64(joinDF.customer_id,joinDF.customer_name) != xxhash64(joinDF.target_customer_id,joinDF.target_customer_name))
#     display(filterDF)

    mergeDF = filterDF.withColumn("MERGEKEY", xxhash64(filterDF.customer_id))
#     display(mergeDF)

    dummyDF = filterDF.filter("target_customer_id IS NOT NULL").withColumn("MERGEKEY", lit(None))
#     display(dummyDF)

    scdDF = mergeDF.union(dummyDF)
#     display(scdDF)

    targetTable.alias("target").merge(
    source = scdDF.alias("source"),
    condition = "xxhash64(target.customer_id) == source.MERGEKEY and target.is_current == 'True'").whenMatchedUpdate(set =
    {
        "target.is_current": "'False'",
        "target.expiration_date": "current_timestamp"
    }
    ).whenNotMatchedInsert(values=
    {
        "target.customer_id": "source.customer_id",
        "target.customer_name": "source.customer_name",
        "target.load_time": "source.load_time",
        "target.is_current": "true",
        "target.effective_date": "current_timestamp",
        "target.expiration_date": "'9999-12-31 23:59:59'"
    }).execute()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver_catalog.customers_db.customers_scd

# COMMAND ----------

# MAGIC %sql
# MAGIC describe history silver_catalog.customers_db.customers_scd

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

# COMMAND ----------

customersDF = spark.read.format("delta").table("silver_catalog.customers_db.customers_scd")
invoicesDF = spark.read.format("delta").table("silver_catalog.invoices_db.invoices")

joinDF = (invoicesDF.join(customersDF, invoicesDF.customer_id == customersDF.customer_id, "left")
        .select(
        invoicesDF["*"],  # Select all columns from invoicesDF
        customersDF["customer_name"],  # Select customer_name from customersDF
    ))
display(joinDF)

# COMMAND ----------

summary_df = (joinDF.groupBy("country", "customer_id","customer_name","invoice_year","invoice_month","invoice_date").agg(sum(expr("quantity * unit_price")).alias("TotalAmount"),sum("quantity").alias("TotalQuantity"))
)

summary_df = (summary_df.withColumn("TotalAmount", round(col("TotalAmount"), 2)).orderBy("customer_id","invoice_year","invoice_month").select("country","customer_id","customer_name","invoice_year","invoice_month","invoice_date","TotalQuantity","TotalAmount")
)
display(summary_df)

summary_df.write.format("delta").mode("overwrite").saveAsTable("gold_catalog.sales_db.sales")

# sum("quantity").alias("TotalQuantity")

