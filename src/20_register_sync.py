# Databricks notebook source
# MAGIC %md
# MAGIC # 20 - Meter & business-service register sync (daily)  [ANZ]
# MAGIC Daily snapshot of the register (tenant, business service, meter type, firmware, comms type, status) MERGEd into the
# MAGIC `dim_meter_register` Delta table. ~2% of meters change firmware, 0.5% change status, 0.1% are new each day.

# COMMAND ----------
import common
from pyspark.sql import functions as F

p = common.params(dbutils)
b = common.Bench(spark, p, "register_sync")
reg = common.t(p, "dim_meter_register")
spark.sql(f"CREATE OR REPLACE TABLE {reg} AS SELECT * FROM {common.t(p, 'meters')}")

# COMMAND ----------
n = p["n_platform_meters"]
snap = common.meters_frame(spark, p)
snap = (snap.withColumn("firmware", F.when(F.pmod(F.hash("meter_id"), F.lit(50)) == 0, F.lit("2.2.1")).otherwise(F.col("firmware")))
            .withColumn("status", F.when(F.pmod(F.hash("meter_id", F.lit(1)), F.lit(200)) == 0, F.lit("FAULT")).otherwise(F.col("status"))))
new = common.meters_frame(spark, dict(p, n_platform_meters=n // 1000)).withColumn("meter_id", F.col("meter_id") + n)
snap = snap.unionByName(new)
snap.createOrReplaceTempView("register_snapshot")

with b.step("merge", rows_in=n + n // 1000) as s:
    spark.sql(f"""MERGE INTO {reg} t USING register_snapshot s ON t.meter_id = s.meter_id
                  WHEN MATCHED AND (t.firmware <> s.firmware OR t.status <> s.status) THEN UPDATE SET *
                  WHEN NOT MATCHED THEN INSERT *""")
    s["rows_out"] = spark.table(reg).count()
