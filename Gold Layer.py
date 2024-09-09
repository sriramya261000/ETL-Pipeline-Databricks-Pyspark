# Databricks notebook source
from pyspark.sql.functions import *

# COMMAND ----------

customersDF = spark.read.format("delta").table("silver_catalog.customers_db.customers_scd") \
            .filter("is_current is true")
invoicesDF = spark.read.format("delta").table("silver_catalog.invoices_db.invoices")

joinDF = (invoicesDF.join(customersDF, invoicesDF.customer_id == customersDF.customer_id, "left")
        .select(
        invoicesDF["*"],  # Select all columns from invoicesDF
        customersDF["customer_name"],  # Select customer_name from customersDF
    ))
display(joinDF)

summary_df = (joinDF.groupBy("country", "customer_id","customer_name","invoice_year","invoice_month","invoice_date").agg(sum(expr("quantity * unit_price")).alias("TotalAmount"),sum("quantity").alias("TotalQuantity"))
)
summary_df = (summary_df.withColumn("TotalAmount", round(col("TotalAmount"), 2)).orderBy("customer_id","invoice_year","invoice_month").select("country","customer_id","customer_name","invoice_year","invoice_month","invoice_date","TotalQuantity","TotalAmount")
)
display(summary_df)

summary_df.write.format("delta").mode("overwrite").saveAsTable("gold_catalog.sales_db.sales")


# COMMAND ----------


