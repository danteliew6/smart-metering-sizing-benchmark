# Databricks notebook source
# MAGIC %md
# MAGIC # 30 - Transformer & substation aggregation + load stress (continuous 5-min)  [USA]
# MAGIC Two measurements:
# MAGIC * `full_day_batch` - aggregate a whole day meter -> transformer -> substation (reprocessing / backfill cost);
# MAGIC * `incremental_5min` - aggregate ONE 5-minute slot. The sizing model prices this as an always-on cluster; if a slot
# MAGIC   takes seconds, the duty cycle (seconds / 300) shows how much of an always-on cluster is idle.

# COMMAND ----------
import common
from pyspark.sql import functions as F

p = common.params(dbutils, {"market": "usa", "n_channels": 21})
b = common.Bench(spark, p, "transformer_aggregation")
iv, meters = common.t(p, "interval_readings"), common.t(p, "meters")
day = common.data_days(spark, p)[-1]
CAPACITY_KW = 2.45 * 2.2     # nominal transformer rating used for the load-stress ratio


def aggregate(df):
    j = df.where("status = 'OK'").join(spark.table(meters).select("meter_id", "transformer_id", "substation_id"), "meter_id")
    tr = (j.groupBy("transformer_id", "substation_id", "ts")
           .agg(F.sum("c03").alias("load_kw"), F.avg("c01").alias("avg_voltage"), F.count("*").alias("meters_reporting"))
           .withColumn("load_stress", (F.col("load_kw") / CAPACITY_KW) ** 2))
    return tr


# COMMAND ----------
with b.step("full_day_batch") as s:
    day_df = spark.table(iv).where(f"reading_date = '{day}'")
    s["rows_in"] = day_df.count()
    tr = aggregate(day_df)
    tr.writeTo(common.t(p, "result_transformer_5min")).createOrReplace()
    sub = (spark.table(common.t(p, "result_transformer_5min")).groupBy("substation_id", "ts")
           .agg(F.sum("load_kw").alias("load_kw"), F.avg("avg_voltage").alias("avg_voltage")))
    sub.writeTo(common.t(p, "result_substation_5min")).createOrReplace()
    s["rows_out"] = spark.table(common.t(p, "result_transformer_5min")).count()

# COMMAND ----------
# one 5-minute slot: slot 150 (12:30)
with b.step("incremental_5min") as s:
    ts = f"{day} 12:30:00"
    slot_df = spark.table(iv).where(f"reading_date = '{day}' AND ts = '{ts}'")
    s["rows_in"] = slot_df.count()
    aggregate(slot_df).write.mode("overwrite").saveAsTable(common.t(p, "result_transformer_slot"))
    s["rows_out"] = spark.table(common.t(p, "result_transformer_slot")).count()
