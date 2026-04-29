# Databricks notebook source
# MAGIC %run ./00_common_dq_functions

# COMMAND ----------

from pyspark.sql.functions import col, to_timestamp

# COMMAND ----------

spark.sql("USE CATALOG dev")
spark.sql("USE SCHEMA bronze")

bronze_db="bronze"
silver_db="silver"

# COMMAND ----------

def dq_not_null(df, required_cols):
    condition = None  # Initialize before loop
    for c in required_cols:
        cond = col(c).isNull()
        condition = cond if condition is None else (condition | cond)
    bad_df = df.filter(condition)
    good_df = df.filter(~condition)
    return good_df, bad_df

# COMMAND ----------

bh_bz = spark.table(f"{bronze_db}.batch_header_raw")

bh_std = (bh_bz
    .withColumn("batch_id", col("batch_id").cast("string"))
)

bh_good1, bh_bad1 = dq_not_null(bh_std, ["batch_id"])
bh_sl,    bh_bad2 = dq_dedup(bh_good1, ["batch_id"])

bh_bad_all = dq_union_bad(bh_bad1, bh_bad2)
dq_write_quarantine(bh_bad_all, f"{silver_db}.dq_quarantine_batch_header")

bh_sl.write.mode("overwrite").format("delta").saveAsTable(f"{silver_db}.batch_header_sl")

# COMMAND ----------

eve_bz = spark.table(f"{bronze_db}.fact_events_src_raw")

eve_std = (eve_bz
    .withColumn("batch_id", col("batch_id").cast("string"))
    .withColumn("event_type", col("event_type").cast("string"))
    .withColumn("event_time", to_timestamp(col("event_time")))
)


for ncol in ["event_value", "qty", "amount"]:
    if ncol in eve_std.columns:
        eve_std = eve_std.withColumn(ncol, col(ncol).cast("double"))

# Not null checks
ev_good1, ev_bad1 = dq_not_null(eve_std, ["batch_id", "event_type", "event_time"])

# Dedup checks
ev_good2, ev_bad2 = dq_dedup(ev_good1, ["batch_id", "event_time", "event_type"])

# Timeliness check (no future timestamps)
ev_good3, ev_bad3 = dq_timeliness_no_future(ev_good2, "event_time")

# FK check
bh_sl_df = spark.table(f"{silver_db}.batch_header_sl")
ev_sl, ev_bad4 = dq_fk_exists(ev_good3, "batch_id", bh_sl_df, "batch_id")

# Union all event bad rows + write quarantine
ev_bad_all = dq_union_bad(ev_bad1, ev_bad2, ev_bad3, ev_bad4)
dq_write_quarantine(ev_bad_all, f"{silver_db}.dq_quarantine_fact_events_src")

# Write Silver fact table
ev_sl.write.mode("overwrite").format("delta").saveAsTable(f"{silver_db}.fact_events_src_sl")

# COMMAND ----------

# DBTITLE 1,Fix syntax error: add missing parenthesis to display()
print("batch_header_sl:", spark.table(f"{silver_db}.batch_header_sl").count())
print("dq_quarantine_batch_header:", spark.table(f"{silver_db}.dq_quarantine_batch_header").count())

print("fact_events_src_sl:", spark.table(f"{silver_db}.fact_events_src_sl").count())
print("dq_quarantine_fact_events_src:", spark.table(f"{silver_db}.dq_quarantine_fact_events_src").count())

display(spark.table(f"{silver_db}.fact_events_src_sl"))
