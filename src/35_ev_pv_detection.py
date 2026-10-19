# Databricks notebook source
# MAGIC %md
# MAGIC # 35 - EV detection and PV detection (daily)  [USA]
# MAGIC Rule-based stand-ins over the full history window (the customer's detection models are not available):
# MAGIC * EV: a >= 5 kW step in active power that lasts >= 1 hour in the evening, on >= 3 days.
# MAGIC * PV: midday (10-14h) load materially below the morning (6-9h) load on most days (export / generation signature).

# COMMAND ----------
import common
from pyspark.sql import functions as F, Window

p = common.params(dbutils, {"market": "usa", "n_channels": 21})
iv = common.t(p, "interval_readings")

# COMMAND ----------
b = common.Bench(spark, p, "ev_detection")
with b.step("ev_detect") as s:
    d = spark.table(iv).where("status = 'OK' AND hour(ts) BETWEEN 17 AND 23").select("meter_id", "reading_date", "ts", "c03")
    s["rows_in"] = d.count()
    w = Window.partitionBy("meter_id", "reading_date").orderBy("ts")
    d = d.withColumn("step_kw", F.col("c03") - F.lag("c03", 12).over(w))              # vs one hour earlier
    ev_days = d.groupBy("meter_id", "reading_date").agg((F.max("step_kw") >= 5.0).alias("has_step"))
    out = ev_days.groupBy("meter_id").agg(F.sum(F.col("has_step").cast("int")).alias("days_with_step")).where("days_with_step >= 3")
    out.writeTo(common.t(p, "result_ev_detected")).createOrReplace()
    s["rows_out"] = spark.table(common.t(p, "result_ev_detected")).count()

# COMMAND ----------
b2 = common.Bench(spark, p, "pv_detection")
with b2.step("pv_detect") as s:
    d = spark.table(iv).where("status = 'OK' AND hour(ts) BETWEEN 6 AND 14")
    s["rows_in"] = d.count()
    g = (d.groupBy("meter_id", "reading_date")
          .agg(F.avg(F.when(F.hour("ts").between(6, 9), F.col("c03"))).alias("morning"),
               F.avg(F.when(F.hour("ts").between(10, 14), F.col("c03"))).alias("midday")))
    out = (g.groupBy("meter_id").agg(F.avg((F.col("midday") < F.col("morning") * 0.8).cast("int")).alias("share_days_dip"))
            .where("share_days_dip >= 0.6"))
    out.writeTo(common.t(p, "result_pv_detected")).createOrReplace()
    s["rows_out"] = spark.table(common.t(p, "result_pv_detected")).count()
