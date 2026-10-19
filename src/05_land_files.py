# Databricks notebook source
# MAGIC %md
# MAGIC # 05 - Land JSON files for the ingestion benchmarks
# MAGIC Writes `n_batches` consecutive 15-minute windows of interval readings (3 x 5-min slots for every interval meter) and the
# MAGIC matching conversation-log rows into a UC volume, one folder per 15-minute batch - the shape an upstream head-end would deliver.

# COMMAND ----------
import common

p = common.params(dbutils, {"n_batches": 8})
b = common.Bench(spark, p, "land_files")
day = common.day_list(p)[-1]

# COMMAND ----------
for k in range(p["n_batches"]):
    lo, hi = k * 3, k * 3 + 3                                  # 15 minutes = 3 slots
    with b.step(f"interval_batch_{k:04d}") as s:
        df = common.interval_frame(spark, p, day, lo, hi).drop("reading_date")
        df.repartition(8).write.mode("overwrite").json(common.vol(p, "interval", f"batch_{k:04d}"))
        s["rows_out"] = df.count()
    with b.step(f"conversation_batch_{k:04d}") as s:
        n_rows = p["n_platform_meters"] * 6 // 96              # conversations arrive evenly over 96 windows/day
        df = common.conversation_frame(spark, p, day, n_rows=n_rows, slot_lo=lo, slot_hi=hi).drop("reading_date")
        df.repartition(2).write.mode("overwrite").json(common.vol(p, "conversation", f"batch_{k:04d}"))
        s["rows_out"] = n_rows
