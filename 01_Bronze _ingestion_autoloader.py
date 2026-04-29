# Databricks notebook source
# DBTITLE 1,Cell 1
from pyspark.sql.functions import col, current_timestamp

base_dir = "/Volumes/dev/bronze/landing_zone"
product = "product"  # Assuming 'product' is a string literal as per the context
raw_dir = f"{base_dir}"

print(f"Raw dir: {raw_dir}")

bronze_schema = "dev.bronze"

state_dir = f"{base_dir}/_autoloader_state/bronze_product_demo"

print(f"State dir: {state_dir}")

# COMMAND ----------

# DBTITLE 1,Cell 2
def autoloader_csv_stream(source_dir: str, file_glob: str, schema_location: str):
    return (
        spark.readStream
            .format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("cloudFiles.schemaLocation", schema_location)
            .option("cloudFiles.inferColumnTypes", "true")
            .option("cloudFiles.includeExistingFiles", "true")  
            .option("header", "true")
            .option("pathGlobFilter", file_glob)            
            .load(source_dir)
            .withColumn("ingest_ts", current_timestamp())
            .withColumn("source_file", col("_metadata.file_path"))
    )

# COMMAND ----------

def write_to_bronze(stream_df, table_name: str, checkpoint_location: str):
    return(
        stream_df.writeStream
            .format("delta")
            .option("checkpointLocation", checkpoint_location)
            .option("mergeSchema", "true")     # allow schema evolution for demo purposes
            .trigger(availableNow=True)        # process all available files then stop
            .toTable(table_name)
    )

# COMMAND ----------

# MAGIC %md
# MAGIC product master 
# MAGIC

# COMMAND ----------

pm_stream = autoloader_csv_stream(
    source_dir=raw_dir,
    file_glob="dim_product_master*.csv",
    schema_location=f"{state_dir}/schemas/product_master_raw"
)
write_to_bronze(
    stream_df=pm_stream,
    table_name=f"{bronze_schema}.dim_product_master_raw",
    checkpoint_location=f"{state_dir}/checkpoints/product_master_raw"
)


# COMMAND ----------

#product_attributes 
pa_stream = autoloader_csv_stream(
  source_dir=raw_dir,
  file_glob="dim_product_attributes*.csv",
  schema_location= f'{state_dir}/schemas/product_attributes',
)
write_to_bronze(
  stream_df=pa_stream,
  table_name=f'{bronze_schema}.dim_product_attributes_raw',
  checkpoint_location=f'{state_dir}/checkpoints/product_attributes'
)

# COMMAND ----------

#batch header 

bh_stream = autoloader_csv_stream(
    source_dir =raw_dir,
    file_glob="batch_header*.csv",
    schema_location= f'{state_dir}/schemas/batch_header_raw',
)

write_to_bronze(
    stream_df=bh_stream,
    table_name=f'{bronze_schema}.batch_header_raw',
    checkpoint_location=f'{state_dir}/checkpoints/batch_header_raw'
)


# COMMAND ----------

# DBTITLE 1,Cell 8
#dim equipment 

eq_stream= autoloader_csv_stream(
    source_dir=raw_dir,
    file_glob="dim_equipment*.csv",
    schema_location= f'{state_dir}/schemas/equipment_raw',
)

write_to_bronze(
    stream_df= eq_stream,
    table_name=f'{bronze_schema}.dim_equipment_raw',
    checkpoint_location=f'{state_dir}/checkpoint/equipment_raw'
)


# COMMAND ----------

#dim site 

site_stream=autoloader_csv_stream(
    source_dir= raw_dir,
    file_glob="dim_site*.csv",
    schema_location=f'{state_dir}/schemas/site_raw'
)

write_to_bronze(
    stream_df= site_stream,
    table_name=f'{bronze_schema}.dim_site_raw',
    checkpoint_location=f'{state_dir}/checkpoints/site_raw'
)

# COMMAND ----------

# DBTITLE 1,Untitled
#Fact Events 

fe_stream=autoloader_csv_stream(
    source_dir= raw_dir,
    file_glob="fact_events_src.csv",
    schema_location=f'{state_dir}/schemas/fact_events_src_raw'
)

write_to_bronze(
    stream_df= fe_stream,
    table_name=f'{bronze_schema}.fact_events_src_raw',
    checkpoint_location=f'{state_dir}/checkpoints/fact_events_src_raw'
)


# COMMAND ----------

# DBTITLE 1,Cell 11
#facts_qc_tests 

fq_stream=autoloader_csv_stream(
    source_dir= raw_dir,
    file_glob="fact_qc_tests_src.csv",
    schema_location=f'{state_dir}/schemas/fact_qc_tests_src_raw'
)

write_to_bronze(
    stream_df= fq_stream,
    table_name=f'{bronze_schema}.fact_qc_tests_src_raw',
    checkpoint_location=f'{state_dir}/checkpoints/fact_qc_tests_src_raw'
)


# COMMAND ----------

# DBTITLE 1,Fix missing commas in tables list
tables = [
    "dim_product_master_raw",
    "fact_qc_tests_src_raw",
    "fact_events_src_raw",
    "dim_site_raw",
    "dim_equipment_raw",
    "batch_header_raw",
    "dim_product_attributes_raw",
]

for t in tables:
    full_name = f"{bronze_schema}.{t}"
    cnt = spark.table(full_name).count()
    print(f"{full_name}: {cnt}")


# COMMAND ----------

display(spark.table(f"{bronze_schema}.dim_product_master_raw").limit(10))
display(spark.table(f"{bronze_schema}.fact_qc_tests_src_raw").limit(10))
display(spark.table(f"{bronze_schema}.fact_events_src_raw").limit(10))
display(spark.table(f"{bronze_schema}.dim_site_raw").limit(10))
display(spark.table(f"{bronze_schema}.dim_equipment_raw").limit(10))
display(spark.table(f"{bronze_schema}.batch_header_raw").limit(10))
display(spark.table(f"{bronze_schema}.dim_product_attributes_raw").limit(10))

print(" Auto Loader Bronze ingestion complete (AvailableNow).")
