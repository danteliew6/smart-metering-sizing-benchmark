# Databricks notebook source
# MAGIC %md
# MAGIC # 11 - Conversation log ingestion (Auto Loader, triggered micro-batch)
# MAGIC One row per conversation (meter, tenant, start/end, outcome, failure reason). `batches` 15-minute folders per run.

# COMMAND ----------
import common
from pyspark.sql import functions as F, types as T

p = common.params(dbutils, {"batches": 1})
b = common.Bench(spark, p, "conversation_ingestion")
schema = T.StructType([T.StructField("meter_id", T.LongType()), T.StructField("tenant_id", T.StringType()),
                       T.StructField("start_ts", T.TimestampType()), T.StructField("end_ts", T.TimestampType()),
                       T.StructField("outcome", T.StringType()), T.StructField("failure_reason", T.StringType())])
paths = ",".join(f"batch_{k:04d}" for k in range(p["batches"]))
src = common.vol(p, "conversation", "{" + paths + "}") if p["batches"] > 1 else common.vol(p, "conversation", "batch_0000")
target = common.t(p, f"bronze_conversation_{p['bench_run_id']}")
ckpt = common.vol(p, "_checkpoints", f"conversation_{p['bench_run_id']}")

# COMMAND ----------
with b.step("autoloader_availableNow", note=f"batches={p['batches']}") as s:
    q = (spark.readStream.format("cloudFiles").option("cloudFiles.format", "json")
         .option("cloudFiles.schemaLocation", ckpt + "/schema").schema(schema).load(src)
         .withColumn("reading_date", F.to_date("start_ts"))
         .writeStream.option("checkpointLocation", ckpt + "/cp").trigger(availableNow=True).toTable(target))
    q.awaitTermination()
    s["rows_out"] = spark.table(target).count()
spark.sql(f"DROP TABLE IF EXISTS {target}")
