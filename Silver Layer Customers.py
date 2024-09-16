# Databricks notebook source
# MAGIC %md
# MAGIC ##### 1. Load customers data from the bronze layer into the silver layer and implement Slowly Changing Dimension (SCD) Type 2 on the customers data.

# COMMAND ----------

from delta.tables import DeltaTable
from pyspark.sql.functions import col, lit, date_format, current_timestamp, to_utc_timestamp, xxhash64

timestamp_format = "yyyy-MM-dd HH:mm:ss.SSS"
dummy_timestamp = "9999-12-31 23:59:59"

def table_exists(db_name, table_name):
    tables = spark.catalog.listTables(db_name)
    table_names = [table.name for table in tables]
    return table_name in table_names

def load_source_data():
    return (spark.read.format("delta").table("bronze_catalog.customers_db.customers_raw")
            .filter("CustomerID is not null")
            .dropDuplicates()
            .withColumn("load_time", to_utc_timestamp(col("load_time"), "UTC"))
            .selectExpr("CustomerID as customer_id", "CustomerName as customer_name",
                        "email","ip_address","gender","SSN as ssn","load_time"))

def create_initial_scd(df):
    return (df
            .withColumn("effective_date", date_format(current_timestamp(), timestamp_format))
            .withColumn("expiration_date", date_format(lit(dummy_timestamp), timestamp_format))
            .withColumn("is_current", lit(True))
            .withColumn("load_time", to_utc_timestamp(col("load_time"), "UTC"))
            .select("customer_id", "customer_name", "email","ip_address","gender","ssn","load_time", "effective_date", "expiration_date", "is_current"))

def merge_scd_table(source_df, target_table_name):
    target_table = DeltaTable.forName(spark, target_table_name)
    target_df = target_table.toDF()

    join_df = source_df.join(
        target_df, 
        (source_df.customer_id == target_df.customer_id) & (target_df.is_current == True), 
        "left"
    ).select(
        source_df["*"],
        target_df.customer_id.alias("target_customer_id"),
        target_df.customer_name.alias("target_customer_name"),
        target_df.email.alias("target_email"),
        target_df.ip_address.alias("target_ip_address"),
        target_df.gender.alias("target_gender"),
        target_df.ssn.alias("target_ssn")
    )

    # display(join_df)

    filter_df = join_df.filter(
        xxhash64(join_df.customer_id, join_df.customer_name,join_df.email,join_df.ip_address,join_df.gender,join_df.ssn) != xxhash64(join_df.target_customer_id, join_df.target_customer_name,join_df.target_email,join_df.target_ip_address,join_df.target_gender,join_df.target_ssn)
    )
    # display(filter_df)

    merge_df = filter_df.withColumn("MERGEKEY", xxhash64(filter_df.customer_id))
    # display(merge_df)

    dummy_df = filter_df.filter("target_customer_id IS NOT NULL").withColumn("MERGEKEY", lit(None))
    # display(dummy_df)

    scd_df = merge_df.union(dummy_df)
    # display(scd_df)

    target_table.alias("target").merge(
        source=scd_df.alias("source"),
        condition="xxhash64(target.customer_id) == source.MERGEKEY and target.is_current == 'True'"
    ).whenMatchedUpdate(
        set={
            "target.is_current": "'False'",
            "target.expiration_date": "current_timestamp"
        }
    ).whenNotMatchedInsert(
        values={
            "target.customer_id": "source.customer_id",
            "target.customer_name": "source.customer_name",
            "target.email": "source.email",
            "target.ip_address": "source.ip_address",
            "target.gender": "source.gender",
            "target.ssn": "source.ssn",
            "target.load_time": "source.load_time",
            "target.is_current": "true",
            "target.effective_date": "current_timestamp",
            "target.expiration_date": "'9999-12-31 23:59:59'"
        }
    ).execute()

def main():
    if not table_exists("silver_catalog.customers_db", "customers_scd"):
        print("Initial load")
        source_df = load_source_data()
        initial_scd_df = create_initial_scd(source_df)
        initial_scd_df.write.format("delta").saveAsTable("silver_catalog.customers_db.customers_scd")
    else:
        print("Not initial load")
        source_df = load_source_data()
        merge_scd_table(source_df, "silver_catalog.customers_db.customers_scd")

if __name__ == "__main__":
    main()


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from silver_catalog.customers_db.customers_scd;
