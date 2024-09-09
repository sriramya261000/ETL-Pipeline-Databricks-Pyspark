# Databricks notebook source
from pyspark.sql.functions import *

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
