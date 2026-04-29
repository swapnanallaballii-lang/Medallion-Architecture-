# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.functions import col, to_date, count


spark.sql("USE CATALOG dev")

silver_db = "silver"
gold_db   = "gold"

# COMMAND ----------

spark.table(f"{silver_db}.dim_product_sl") \
     .write.mode("overwrite").format("delta") \
     .saveAsTable(f"{gold_db}.dim_product")

spark.table(f"{silver_db}.dim_equipment_sl") \
     .write.mode("overwrite").format("delta") \
     .saveAsTable(f"{gold_db}.dim_equipment")

spark.table(f"{silver_db}.dim_site_sl") \
     .write.mode("overwrite").format("delta") \
     .saveAsTable(f"{gold_db}.dim_site")

# COMMAND ----------

bhsl = spark.table(f"{silver_db}.batch_header_sl")
evsl = spark.table(f"{silver_db}.fact_events_src_sl")

fact_batch_quality_events = (
    bh.join(ev.drop('_rescued_data', 'ingest_file_date', 'ingest_ts', 'source_file', 'source_system'), on="batch_id", how="inner")
)

fact_batch_quality_events.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable(f"{gold_db}.fact_batch_quality_events")

# COMMAND ----------

qcsl = spark.table(f"{silver_db}.fact_qc_tests_src_sl")

fact_batch_qc_tests = (
    bh.join(qcsl.drop('_rescued_data','ingest_file_date','ingest_ts', 'source_file','source_system'), on="batch_id", how="inner")
)

fact_batch_qc_tests.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable(f"{gold_db}.fact_batch_qc_tests")


# COMMAND ----------

kpi_daily_oos = (
    spark.table(f"{gold_db}.fact_batch_quality_events")
        .filter(col("event_type") == "OOS")
        .withColumn("event_date", to_date(col("event_time")))
        .groupBy(
            "event_date",
            "site_id",
            "product_id"
        )
        .agg(
            count("*").alias("oos_count")
        )
)

kpi_daily_oos.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable(f"{gold_db}.kpi_daily_oos")


# COMMAND ----------

# DBTITLE 1,Fix table references to use dev.gold schema
print("dim_product:", spark.table("dev.gold.dim_product").count())
print("dim_site:", spark.table("dev.gold.dim_site").count())
print("dim_equipment:", spark.table("dev.gold.dim_equipment").count())

print("fact_batch_quality_events:",
      spark.table("dev.gold.fact_batch_quality_events").count())

print("fact_batch_qc_tests:",
      spark.table("dev.gold.fact_batch_qc_tests").count())

print("kpi_daily_oos:",
      spark.table("dev.gold.kpi_daily_oos").count())

