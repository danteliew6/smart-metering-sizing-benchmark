# Databricks notebook source
# MAGIC %md
# MAGIC # 34 - Estimation of missing data (continuous 5-min)  [USA]
# MAGIC Failed intervals are estimated from the same slot on the previous day (simple algorithm; a model would cost more).

# COMMAND ----------
import common
from pyspark.sql import functions as F

p = common.params(dbutils, {"market": "usa", "n_channels": 21})
b = common.Bench(spark, p, "missing_data_estimation")
iv = common.t(p, "interval_readings")
days = common.data_days(spark, p)

# COMMAND ----------
with b.step("estimate_one_day") as s:
    today_df = spark.table(iv).where(f"reading_date = '{days[-1]}'")
    prev = (spark.table(iv).where(f"reading_date = '{days[-2]}' AND status = 'OK'")
            .select("meter_id", (F.col("ts") + F.expr("INTERVAL 1 DAY")).alias("ts"), F.col("c01").alias("p_c01"), F.col("c03").alias("p_c03")))
    s["rows_in"] = today_df.count()
    est = (today_df.where("status = 'FAILED'").select("meter_id", "ts").join(prev, ["meter_id", "ts"], "left")
           .withColumn("estimated", F.lit(True)))
    est.writeTo(common.t(p, "result_estimated_intervals")).createOrReplace()
    s["rows_out"] = spark.table(common.t(p, "result_estimated_intervals")).count()
