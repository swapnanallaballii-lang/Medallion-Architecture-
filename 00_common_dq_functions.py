# Databricks notebook source
from pyspark.sql import functions
from pyspark.sql import DataFrame
from pyspark.sql.functions import (col, lit, when, concat_ws, current_timestamp, to_timestamp, row_number)
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number
from functools import reduce

# COMMAND ----------

def dq_not_null(df: DataFrame, required_cols: list):
    condition: None
    for c in required_cols:
        cond=col(c).isNull()
        condition = cond if condition is None else (condition | cond)
    bad_df = df.filter(condition)
    good_df = df.filter(~condition)

    return good_df, bad_df



# COMMAND ----------

def dq_dedup(df: DataFrame, key_cols: list):
    """
    Returns:
      dedup_df: first record per key
      dup_df  : duplicate records
    """
    window_spec = Window.partitionBy(*key_cols).orderBy(current_timestamp())

    df_rn = df.withColumn("_rn", row_number().over(window_spec))

    dedup_df = df_rn.filter(col("_rn") == 1).drop("_rn")
    dup_df   = df_rn.filter(col("_rn") > 1).drop("_rn")

    return dedup_df, dup_df

# COMMAND ----------

def dq_timeliness_no_future(df: DataFrame, ts_col: str):
    """
    Returns:
      good_df: timestamp <= current_timestamp
      bad_df : timestamp > current_timestamp
    """
    df_ts = df.withColumn("_ts_tmp", to_timestamp(col(ts_col)))

    bad_df = df_ts.filter(col("_ts_tmp") > current_timestamp()).drop("_ts_tmp")
    good_df = df_ts.filter(
        (col("_ts_tmp").isNull()) | (col("_ts_tmp") <= current_timestamp())
    ).drop("_ts_tmp")

    return good_df, bad_df

# COMMAND ----------

def dq_fk_exists(df: DataFrame, fk_col: str, dim_df: DataFrame, dim_key: str):
    """
    Returns:
      good_df: FK exists in dimension
      bad_df : FK missing in dimension
    """
    dim_keys = dim_df.select(col(dim_key).alias("_dim_key")).distinct()

    joined = df.join(
        dim_keys,
        df[fk_col] == col("_dim_key"),
        "left"
    )

    bad_df = joined.filter(
        col(fk_col).isNotNull() & col("_dim_key").isNull()
    ).drop("_dim_key")

    good_df = joined.filter(
        col("_dim_key").isNotNull() | col(fk_col).isNull()
    ).drop("_dim_key")

    return good_df, bad_df


# COMMAND ----------

def dq_union_bad(*bad_dfs):
    """
    Returns:
      Single DataFrame containing all bad rows
    """
    return reduce(
        lambda a, b: a.unionByName(b, allowMissingColumns=True),
        bad_dfs
    )




# COMMAND ----------

def dq_write_quarantine(bad_df: DataFrame, quarantine_table: str):
    """
    Appends failed rows to Delta quarantine table
    """
    (
        bad_df
        .withColumn("_dq_ts", current_timestamp())
        .write
        .mode("append")
        .format("delta")
        .saveAsTable(quarantine_table)
    )
