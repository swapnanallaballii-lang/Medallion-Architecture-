# Databricks notebook source
# MAGIC %run ./00_common_dq_functions

# COMMAND ----------

# MAGIC %run ./00_common_dq_functions

# COMMAND ----------

import pyspark.sql.functions as F

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp
import pyspark.sql.functions as F
spark.sql("USE CATALOG dev")
spark.sql("USE SCHEMA bronze")

bronze_db="bronze"
silver_db="silver"

# COMMAND ----------

#join productmaster and product attributes
pm = spark.table(f"{bronze_db}.dim_product_master_raw")
pa = spark.table(f"{bronze_db}.dim_product_attributes_raw")

dim_product = (pm.alias("pm")
               .join(pa.alias("pa"), "product_id", "left")
               .select(
                   "product_id",
                   "product_name",
                   "dosage_form",        
                   col("pa.strength").alias("strength"),
                   col("pa.container").alias("container"),
                   current_timestamp().alias("dim_load_ts")
               )
              )

dim_product.write.mode("overwrite").format("delta").saveAsTable(f"{silver_db}.dim_product_sl")
display(spark.table(f"{silver_db}.dim_product_sl").limit(20))


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

# MAGIC %sql
# MAGIC ALTER TABLE silver.dq_quarantine_dim_site ADD COLUMN _dq_ts TIMESTAMP

# COMMAND ----------

from pyspark.sql import functions as F
site_bz = spark.table(f"{bronze_db}.dim_site_raw")

site_std = site_bz.withColumn("site_id", col("site_id").cast("string"))

#Pk is not null 
site_good1, site_bad1 = dq_not_null(site_std, ["site_id"])

#Quarantine
site_bad_all = dq_union_bad(site_bad1)
dq_write_quarantine(site_bad_all, f"{silver_db}.dq_quarantine_dim_site")

site_good1.write.mode("overwrite").format("delta").saveAsTable(f"{silver_db}.dim_site_sl")

# COMMAND ----------

from pyspark.sql import functions as F

equ = spark.table(f"{bronze_db}.dim_equipment_raw")

# Standardize types
equip_std = (
    equ
    .withColumn("equipment_id", F.col("equipment_id").cast("string"))
    .withColumn("site_id", F.col("site_id").cast("string"))
)

# DQ: PK not null
equip_good1, equip_bad1 = dq_not_null(equip_std, ["equipment_id"])

# DQ: Deduplicate by PK
equip_good2, equip_bad2 = dq_dedup(equip_good1, ["equipment_id"])

# DQ: FK exists (equipment.site_id must exist in dim_site_sl)
site_sl_keys = spark.table(f"{silver_db}.dim_site_sl")
equip_sl, equip_bad3 = dq_fk_exists(
    equip_good2, "site_id", site_sl_keys, "site_id"
)

# Quarantine
equip_bad_all = dq_union_bad(equip_bad1, equip_bad2, equip_bad3)
dq_write_quarantine(
    equip_bad_all,
    f"{silver_db}.dq_quarantine_dim_equipment"
)

# Write Silver
equip_sl.write.mode("overwrite").format("delta").saveAsTable(
    f"{silver_db}.dim_equipment_sl"
)


# COMMAND ----------

pm = spark.table(f"{bronze_db}.dim_product_master_raw")

# Standardize types
pm_std = pm.withColumn("product_id", col("product_id").cast("string"))

# DQ: PK not null
pm_good1, pm_bad1 = dq_not_null(pm_std, ["product_id"])

# DQ: Deduplicate by PK
pm_sl, pm_bad2 = dq_dedup(pm_good1, ["product_id"])

# Quarantine for product master
pm_bad_all = dq_union_bad(pm_bad1, pm_bad2)
dq_write_quarantine(pm_bad_all, f"{silver_db}.dq_quarantine_dim_product_master")


# COMMAND ----------

pa = spark.table(f"{bronze_db}.dim_product_attributes_raw")

# Standardize types
pa_std = pa.withColumn("product_id", col("product_id").cast("string"))

# DQ: PK not null
pa_good1, pa_bad1 = dq_not_null(pa_std, ["product_id"])

# DQ: Deduplicate by PK
pa_sl, pa_bad2 = dq_dedup(pa_good1, ["product_id"])

# Quarantine for product attributes
pa_bad_all = dq_union_bad(pa_bad1, pa_bad2)
dq_write_quarantine(pa_bad_all, f"{silver_db}.dq_quarantine_dim_product_attributes")
