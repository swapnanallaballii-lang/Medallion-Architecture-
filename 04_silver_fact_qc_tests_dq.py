# Databricks notebook source
# MAGIC %run ./00_common_dq_functions

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

factqc_bz = spark.table(f"{bronze_db}.fact_qc_tests_src_raw")

qc_std = (
    factqc_bz
    .withColumn("batch_id", col("batch_id").cast("string"))
    .withColumn("test_name", col("test_name").cast("string"))
    .withColumn("test_time", to_timestamp(col("test_time")))
)

# Standardize numeric columns if they exist
for ncol in ["test_value", "result_value", "upper_limit", "lower_limit"]:
    if ncol in qc_std.columns:
        qc_std = qc_std.withColumn(ncol, col(ncol).cast("double"))

qc_good1, qc_bad1 = dq_not_null(
    qc_std,
    ["batch_id", "test_name", "test_time"]
)

qc_good2, qc_bad2 = dq_dedup(
    qc_good1,
    ["batch_id", "test_name", "test_time"]
)

qc_good3, qc_bad3 = dq_timeliness_no_future(
    qc_good2,
    "test_time"
)



# COMMAND ----------


batch_header_sl = spark.table(f"{silver_db}.batch_header_sl")

qc_sl, qc_bad4 = dq_fk_exists(
    qc_good3,
    "batch_id",
    batch_header_sl,
    "batch_id"
)

qc_bad_all = dq_union_bad(
    qc_bad1,
    qc_bad2,
    qc_bad3,
    qc_bad4
)

dq_write_quarantine( qc_bad_all,
    f"{silver_db}.dq_quarantine_fact_qc_tests_src"
)

qc_sl.write \
    .mode("overwrite") \
    .format("delta") \
    .saveAsTable(f"{silver_db}.fact_qc_tests_src_sl")


# COMMAND ----------

print("fact_qc_tests_src_sl:", spark.table(f"{silver_db}.fact_qc_tests_src_sl").count())
print("dq_quarantine_fact_qc_tests_src:",
      spark.table(f"{silver_db}.dq_quarantine_fact_qc_tests_src").count())
display(spark.table(f"{silver_db}.fact_qc_tests_src_sl"))
display(spark.table(f"{silver_db}.dq_quarantine_fact_qc_tests_src"))

