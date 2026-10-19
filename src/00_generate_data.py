# Databricks notebook source
# MAGIC %md
# MAGIC # 00 - Generate synthetic smart-meter data
# MAGIC Builds the meter register, `history_days` of 5-minute interval readings, conversation logs and (USA) a 12-month
# MAGIC daily-peak history. Signals for every use case (voltage excursions, poor comms, outages, EV/PV) are seeded - see
# MAGIC `common.interval_frame`.

# COMMAND ----------
import common
from pyspark.sql import functions as F

p = common.params(dbutils)
b = common.Bench(spark, p, "data_generation")
days = common.day_list(p)
print(p, days)

# COMMAND ----------
with b.step("meters") as s:
    common.meters_frame(spark, p).writeTo(common.t(p, "meters")).createOrReplace()
    s["rows_out"] = spark.table(common.t(p, "meters")).count()

# COMMAND ----------
# One day at a time keeps shuffles small. Liquid clustering on (meter_id, reading_date) serves point lookups.
with b.step("interval_readings", note=f"{len(days)} days") as s:
    first = True
    for d in days:
        df = common.interval_frame(spark, p, d)
        if first:
            df.writeTo(common.t(p, "interval_readings")).clusterBy("meter_id", "reading_date").createOrReplace()
            first = False
        else:
            df.writeTo(common.t(p, "interval_readings")).append()
    s["rows_out"] = spark.table(common.t(p, "interval_readings")).count()

# COMMAND ----------
with b.step("conversations", note=f"{len(days)} days") as s:
    first = True
    for d in days:
        df = common.conversation_frame(spark, p, d)
        if first:
            df.writeTo(common.t(p, "conversations")).clusterBy("reading_date", "meter_id").createOrReplace()
            first = False
        else:
            df.writeTo(common.t(p, "conversations")).append()
    s["rows_out"] = spark.table(common.t(p, "conversations")).count()

# COMMAND ----------
# 12 months of daily transformer peaks (USA peak detection: 1 week / 1 month / 12 months windows).
n_trans = int(p["n_interval_meters"] / 2.45) + 1
with b.step("peak_daily_history") as s:
    (spark.range(1, n_trans + 1).withColumnRenamed("id", "transformer_id")
        .crossJoin(spark.range(365).withColumnRenamed("id", "day_offset"))
        .withColumn("peak_date", F.date_sub(F.lit(days[0]).cast("date"), F.col("day_offset").cast("int") + 1))
        .withColumn("peak_kw", F.lit(2.45) * (2.5 + F.rand(5) * 2.0))
        .drop("day_offset")
        .writeTo(common.t(p, "peak_daily_history")).createOrReplace())
    s["rows_out"] = spark.table(common.t(p, "peak_daily_history")).count()
