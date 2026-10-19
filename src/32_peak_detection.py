# Databricks notebook source
# MAGIC %md
# MAGIC # 32 - Peak detection: actual (daily) and forecasted (4-hourly)  [USA]
# MAGIC Highest interval at transformer level within 1 week, 1 month and 12 months. The 1-week window is computed from the
# MAGIC meter-level history; month / 12-month windows reuse the daily-peak history table.

# COMMAND ----------
import common
from pyspark.sql import functions as F

p = common.params(dbutils, {"market": "usa", "n_channels": 21})
b = common.Bench(spark, p, "peak_detection_actual")
iv, meters = common.t(p, "interval_readings"), common.t(p, "meters")

# COMMAND ----------
with b.step("peak_actual_1w_1m_12m") as s:
    s["rows_in"] = spark.table(iv).count()
    tr = (spark.table(iv).where("status = 'OK'").join(spark.table(meters).select("meter_id", "transformer_id"), "meter_id")
          .groupBy("transformer_id", "reading_date", "ts").agg(F.sum("c03").alias("load_kw")))
    week = tr.groupBy("transformer_id").agg(F.max_by("ts", "load_kw").alias("peak_ts_1w"), F.max("load_kw").alias("peak_kw_1w"))
    daily = spark.table(common.t(p, "peak_daily_history"))
    month = daily.where("peak_date >= date_sub(current_date(), 30)").groupBy("transformer_id").agg(F.max("peak_kw").alias("peak_kw_1m"))
    year = daily.groupBy("transformer_id").agg(F.max("peak_kw").alias("peak_kw_12m"))
    week.join(month, "transformer_id", "left").join(year, "transformer_id", "left").writeTo(common.t(p, "result_peak_actual")).createOrReplace()
    s["rows_out"] = spark.table(common.t(p, "result_peak_actual")).count()

# COMMAND ----------
b2 = common.Bench(spark, p, "peak_detection_forecasted")
with b2.step("peak_forecast") as s:
    f = spark.table(common.t(p, "result_transformer_forecast"))
    s["rows_in"] = f.count()
    f.groupBy("transformer_id").agg(F.max_by("slot", "forecast_load_kw").alias("peak_slot"), F.max("forecast_load_kw").alias("peak_forecast_kw")) \
     .writeTo(common.t(p, "result_peak_forecast")).createOrReplace()
    s["rows_out"] = spark.table(common.t(p, "result_peak_forecast")).count()
