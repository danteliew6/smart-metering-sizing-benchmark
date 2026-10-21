# Databricks notebook source
# MAGIC %md
# MAGIC # 38 - Frequency sweep for "continuous but flexible" use cases  [USA]
# MAGIC Runs ONE incremental micro-batch for a `use_case` over `window_minutes` of fresh readings (plus the lookback the logic needs), so the
# MAGIC cost of running every `window_minutes` can be measured: each run is a separate job run (billed separately), giving
# MAGIC DBU/run = fixed start-up + cost per row read. Use cases:
# MAGIC * `outage` - correlate failed reads per transformer (lookback 1 interval)
# MAGIC * `ev_disagg` / `pv_disagg` - subtract the EV / PV signature for meters flagged by the daily detection (lookback 1 hour)
# MAGIC * `read_perf` - merge window success/failure counts and last good read into a per-meter state table
# MAGIC * `forecast_refresh` - nowcast bias correction of the next 3 hours of the site forecast from the window's actuals

# COMMAND ----------
import common
from pyspark.sql import functions as F, Window

p = common.params(dbutils, {"market": "usa", "n_channels": 21, "use_case": "outage", "window_minutes": 5})
uc, W = p["use_case"], int(p["window_minutes"])
b = common.Bench(spark, p, f"freq_{uc}")
iv, meters = common.t(p, "interval_readings"), common.t(p, "meters")
day = common.data_days(spark, p)[-1]
S0 = 150                                   # window starts at slot 150 (12:30)
n_slots = max(1, W // 5)
LOOKBACK = {"outage": 1, "ev_disagg": 12, "pv_disagg": 12, "read_perf": 0, "forecast_refresh": 0}[uc]


def ts_of(slot):
    return f"{day} {slot * 5 // 60:02d}:{slot * 5 % 60:02d}:00"


lo, hi = ts_of(S0 - LOOKBACK), ts_of(S0 + n_slots)
base = spark.table(iv).where(f"reading_date = '{day}' AND ts >= '{lo}' AND ts < '{hi}'")

# COMMAND ----------
with b.step(f"run_w{W}", note=f"window_minutes={W}") as s:
    if uc == "outage":
        d = base.select("meter_id", "ts", "status").join(spark.table(meters).select("meter_id", "transformer_id"), "meter_id")
        per = d.groupBy("transformer_id", "ts").agg((F.count_if(F.col("status") == "FAILED") / F.count("*")).alias("fail_share"), F.count("*").alias("n"))
        w = Window.partitionBy("transformer_id").orderBy("ts")
        out = (per.withColumn("two_in_row", (F.col("fail_share") >= 0.8) & (F.lag("fail_share").over(w) >= 0.8))
                  .where("two_in_row AND n >= 2").groupBy("transformer_id").agg(F.min("ts").alias("outage_start"), F.max("ts").alias("outage_end")))
        s["rows_in"] = base.count()
    elif uc in ("ev_disagg", "pv_disagg"):
        flagged = spark.table(common.t(p, "result_ev_detected" if uc == "ev_disagg" else "result_pv_detected")).select("meter_id")
        d = base.where("status = 'OK'").join(F.broadcast(flagged), "meter_id").select("meter_id", "ts", "c03")
        w = Window.partitionBy("meter_id").orderBy("ts")
        if uc == "ev_disagg":
            step = F.col("c03") - F.lag("c03", 12).over(w)
            out = d.withColumn("ev_kw", F.when(step >= 5.0, F.least(F.col("c03"), F.lit(7.0))).otherwise(0.0)).withColumn("base_kw", F.col("c03") - F.col("ev_kw"))
        else:
            ref = F.lag("c03", 12).over(w)
            out = d.withColumn("pv_kw", F.greatest(F.lit(0.0), ref - F.col("c03"))).withColumn("net_kw", F.col("c03") + F.col("pv_kw"))
        s["rows_in"] = d.count()
    elif uc == "read_perf":
        agg = (base.groupBy("meter_id").agg(F.count_if(F.col("status") == "OK").alias("ok"), F.count_if(F.col("status") == "FAILED").alias("failed"),
                                           F.max(F.when(F.col("status") == "OK", F.col("ts"))).alias("last_good")))
        state = common.t(p, "state_read_perf")
        spark.sql(f"CREATE TABLE IF NOT EXISTS {state} AS SELECT meter_id, 0L ok, 0L failed, CAST(NULL AS TIMESTAMP) last_good FROM {meters} WHERE has_interval_data")
        agg.createOrReplaceTempView("win_counts")
        spark.sql(f"""MERGE INTO {state} t USING win_counts s ON t.meter_id = s.meter_id
                      WHEN MATCHED THEN UPDATE SET t.ok = t.ok + s.ok, t.failed = t.failed + s.failed, t.last_good = coalesce(s.last_good, t.last_good)""")
        s["rows_in"] = base.count(); out = None
    else:   # forecast_refresh
        fc = spark.table(common.t(p, "result_site_forecast")).where("ahead = 0")
        act = base.where("status = 'OK'").withColumn("slot", (F.hour("ts") * 12 + (F.minute("ts") / 5).cast("int")))
        bias = (act.join(fc, ["meter_id", "slot"]).groupBy("meter_id").agg(F.avg(F.col("c03") - F.col("forecast_kw")).alias("bias")))
        nxt = spark.range(S0 + n_slots, S0 + n_slots + 36).withColumnRenamed("id", "slot")
        out = fc.join(nxt, "slot").join(bias, "meter_id").withColumn("forecast_kw", F.col("forecast_kw") + F.col("bias"))
        s["rows_in"] = act.count()
    if out is not None:
        out.write.mode("overwrite").saveAsTable(common.t(p, f"result_freq_{uc}"))
        s["rows_out"] = spark.table(common.t(p, f"result_freq_{uc}")).count()
    else:
        s["rows_out"] = spark.table(common.t(p, "state_read_perf")).count()
