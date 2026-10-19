# Databricks notebook source
# MAGIC %md
# MAGIC # 12 - Weather (4-hourly) and 3rd-party meter data (daily) ingestion  [USA]
# MAGIC Volumes are not given in the customer file, so these are *estimates*: 500 weather stations x 12 readings per 4-hour pull, and a
# MAGIC daily third-party file covering 10% of the interval-meter count at 5-minute resolution.

# COMMAND ----------
import common
from pyspark.sql import functions as F

p = common.params(dbutils, {"market": "usa", "n_channels": 21})
b = common.Bench(spark, p, "weather_ingestion")
day = common.day_list(p)[-1]

# COMMAND ----------
with b.step("weather_pull_4h") as s:
    w = (spark.range(500).withColumnRenamed("id", "station_id").crossJoin(spark.range(48).withColumnRenamed("id", "slot"))
         .withColumn("ts", F.timestamp_seconds(F.unix_timestamp(F.lit(day).cast("timestamp")) + F.col("slot") * 300))
         .withColumn("temp_c", 10 + F.rand(1) * 25).withColumn("irradiance", F.rand(2) * 1000).withColumn("wind_ms", F.rand(3) * 15))
    w.write.mode("overwrite").json(common.vol(p, "weather", p["bench_run_id"]))
    df = spark.read.json(common.vol(p, "weather", p["bench_run_id"]))
    df.writeTo(common.t(p, "bronze_weather")).createOrReplace()
    s["rows_out"] = df.count()

# COMMAND ----------
b2 = common.Bench(spark, p, "third_party_ingestion")
with b2.step("third_party_daily_file") as s:
    n3 = max(1, p["n_interval_meters"] // 10)
    q = dict(p, n_interval_meters=n3)
    tp = common.interval_frame(spark, q, day).drop("reading_date")
    tp.write.mode("overwrite").json(common.vol(p, "third_party", p["bench_run_id"]))
    df = spark.read.json(common.vol(p, "third_party", p["bench_run_id"]))
    df.writeTo(common.t(p, "bronze_third_party")).createOrReplace()
    s["rows_out"] = df.count()
