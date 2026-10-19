# Databricks notebook source
# MAGIC %md
# MAGIC # 31 - Site forecasts (5-min resolution)  +  Transformer forecast aggregation  [USA]
# MAGIC Stand-in model (the customer's real forecasting model is not available): per meter and 5-min slot, the mean load over
# MAGIC the history window scaled by a day-over-day trend, producing a `forecast_days`-ahead 5-minute forecast.
# MAGIC Real ML forecasting will cost more; treat this as the *floor* for data volume handling.

# COMMAND ----------
import common
from pyspark.sql import functions as F

p = common.params(dbutils, {"market": "usa", "n_channels": 21, "forecast_days": 1})
b = common.Bench(spark, p, "site_forecasts")
iv, meters = common.t(p, "interval_readings"), common.t(p, "meters")
days = common.day_list(p)
fc = common.t(p, "result_site_forecast")

# COMMAND ----------
with b.step("site_forecast") as s:
    hist = spark.table(iv).where("status = 'OK'").withColumn("slot", (F.hour("ts") * 12 + F.minute("ts") / 5).cast("int"))
    prof = hist.groupBy("meter_id", "slot").agg(F.avg("c03").alias("mean_kw"), F.stddev("c03").alias("sd_kw"))
    trend = (hist.groupBy("meter_id", "reading_date").agg(F.avg("c03").alias("d")).groupBy("meter_id")
             .agg((F.max("d") / F.avg("d")).alias("trend")))
    out = (prof.join(trend, "meter_id")
           .crossJoin(spark.range(p["forecast_days"]).withColumnRenamed("id", "ahead"))
           .withColumn("forecast_kw", F.col("mean_kw") * F.col("trend"))
           .select("meter_id", "slot", "ahead", "forecast_kw", "sd_kw"))
    out.writeTo(fc).createOrReplace()
    s["rows_in"] = spark.table(iv).count()
    s["rows_out"] = spark.table(fc).count()

# COMMAND ----------
b2 = common.Bench(spark, p, "transformer_forecast_aggregation")
with b2.step("tfa") as s:
    out = (spark.table(fc).join(spark.table(meters).select("meter_id", "transformer_id"), "meter_id")
           .groupBy("transformer_id", "ahead", "slot").agg(F.sum("forecast_kw").alias("forecast_load_kw")))
    out.writeTo(common.t(p, "result_transformer_forecast")).createOrReplace()
    s["rows_in"] = spark.table(fc).count()
    s["rows_out"] = spark.table(common.t(p, "result_transformer_forecast")).count()
