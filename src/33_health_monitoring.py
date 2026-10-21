# Databricks notebook source
# MAGIC %md
# MAGIC # 33 - Health monitoring: voltage alarms, read performance / last read, comms-failure alarm  [USA]
# MAGIC One pass over the last 24 hours (288 intervals per meter): voltage out of band for 3 consecutive intervals (alarm),
# MAGIC successful-interval percentage and last good read, and a failed-interval threshold alarm.

# COMMAND ----------
import common
from pyspark.sql import functions as F, Window

p = common.params(dbutils, {"market": "usa", "n_channels": 21})
b = common.Bench(spark, p, "health_monitoring")
iv = common.t(p, "interval_readings")
day = common.data_days(spark, p)[-1]
lo, hi = common.NOMINAL_V * (1 - common.BAND), common.NOMINAL_V * (1 + common.BAND)

# COMMAND ----------
with b.step("health_24h") as s:
    d = spark.table(iv).where(f"reading_date = '{day}'")
    s["rows_in"] = d.count()
    w = Window.partitionBy("meter_id").orderBy("ts")
    oob = (F.col("c01") < lo) | (F.col("c01") > hi)
    d = d.withColumn("oob3", (oob.cast("int") + F.lag(oob.cast("int"), 1).over(w) + F.lag(oob.cast("int"), 2).over(w)) == 3)
    out = (d.groupBy("meter_id").agg(F.max("oob3").alias("voltage_alarm"),
            (F.count_if(F.col("status") == "OK") / F.count("*")).alias("read_success_pct"),
            F.max(F.when(F.col("status") == "OK", F.col("ts"))).alias("last_good_read"),
            F.count_if(F.col("status") == "FAILED").alias("failed_intervals"))
           .withColumn("comms_failure_alarm", F.col("failed_intervals") > 0.5 * 288))
    out.writeTo(common.t(p, "result_health")).createOrReplace()
    s["rows_out"] = spark.table(common.t(p, "result_health")).count()
