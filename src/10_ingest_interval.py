# Databricks notebook source
# MAGIC %md
# MAGIC # 10 - Interval data ingestion (Auto Loader, triggered)
# MAGIC Processes the first `batches` landed 15-minute folders with Auto Loader (`availableNow`) into a Bronze Delta table.
# MAGIC * `batches=1`  : one triggered run, the unit the sizing model prices (runs/day x minutes/run).
# MAGIC * `batches=8`  : backlog drain - gives sustained rows/second per compute, used to check the always-on cluster counts.
# MAGIC Run it once per compute variant (serverless, classic m5d.xlarge 1+N) and compare `rows_per_second`.

# COMMAND ----------
import common
from pyspark.sql import functions as F, types as T

p = common.params(dbutils, {"batches": 1})
b = common.Bench(spark, p, "interval_ingestion")
nc = p["n_channels"]

schema = T.StructType([T.StructField("meter_id", T.LongType()), T.StructField("ts", T.TimestampType()),
                       T.StructField("status", T.StringType())] + [T.StructField(f"c{i:02d}", T.DoubleType()) for i in range(1, nc + 1)])
paths = ",".join(f"batch_{k:04d}" for k in range(p["batches"]))
src = common.vol(p, "interval", "{" + paths + "}") if p["batches"] > 1 else common.vol(p, "interval", "batch_0000")
target = common.t(p, f"bronze_interval_{p['bench_run_id']}")
ckpt = common.vol(p, "_checkpoints", f"interval_{p['bench_run_id']}")

# COMMAND ----------
with b.step("autoloader_availableNow", note=f"batches={p['batches']}") as s:
    q = (spark.readStream.format("cloudFiles").option("cloudFiles.format", "json")
         .option("cloudFiles.schemaLocation", ckpt + "/schema").schema(schema).load(src)
         .withColumn("ingested_at", F.current_timestamp()).withColumn("reading_date", F.to_date("ts"))
         .writeStream.option("checkpointLocation", ckpt + "/cp").trigger(availableNow=True).toTable(target))
    q.awaitTermination()
    s["rows_out"] = spark.table(target).count()

# COMMAND ----------
r = spark.table(common.t(p, "bench_results")).where(
    (F.col("bench_run_id") == p["bench_run_id"]) & (F.col("step") == "autoloader_availableNow")).collect()[-1]
print(f"rows_per_second = {r['rows_out'] / r['seconds']:.0f}")
spark.sql(f"DROP TABLE IF EXISTS {target}")      # keep the schema clean between runs
