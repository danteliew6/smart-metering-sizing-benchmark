# Databricks notebook source
# MAGIC %md
# MAGIC # 36 - Outage / restoration detection (continuous 24x7)  [USA]
# MAGIC Correlates failed reads across meters under the same transformer on consecutive intervals; the transformer is the
# MAGIC root node. Run on the last day (where the generator seeds outages) as a batch; `incremental_1h` scans the last hour only,
# MAGIC which is the working set of a continuous implementation.

# COMMAND ----------
import common
from pyspark.sql import functions as F, Window

p = common.params(dbutils, {"market": "usa", "n_channels": 21})
b = common.Bench(spark, p, "outage_detection")
iv, meters = common.t(p, "interval_readings"), common.t(p, "meters")
day = common.data_days(spark, p)[-1]


def detect(df):
    j = df.select("meter_id", "ts", "status").join(spark.table(meters).select("meter_id", "transformer_id"), "meter_id")
    per = j.groupBy("transformer_id", "ts").agg((F.count_if(F.col("status") == "FAILED") / F.count("*")).alias("fail_share"),
                                               F.count("*").alias("n"))
    w = Window.partitionBy("transformer_id").orderBy("ts")
    per = per.withColumn("two_in_row", (F.col("fail_share") >= 0.8) & (F.lag("fail_share").over(w) >= 0.8))
    return per.where("two_in_row AND n >= 2").groupBy("transformer_id").agg(F.min("ts").alias("outage_start"), F.max("ts").alias("outage_end"))


# COMMAND ----------
with b.step("outage_full_day") as s:
    d = spark.table(iv).where(f"reading_date = '{day}'")
    s["rows_in"] = d.count()
    detect(d).writeTo(common.t(p, "result_outages")).createOrReplace()
    s["rows_out"] = spark.table(common.t(p, "result_outages")).count()

# COMMAND ----------
with b.step("incremental_1h") as s:
    d = spark.table(iv).where(f"reading_date = '{day}' AND ts >= '{day} 08:20:00' AND ts < '{day} 09:20:00'")
    s["rows_in"] = d.count()
    detect(d).write.mode("overwrite").saveAsTable(common.t(p, "result_outages_1h"))
    s["rows_out"] = spark.table(common.t(p, "result_outages_1h")).count()
