# Databricks notebook source
# MAGIC %md
# MAGIC # 37 - Weekly forecasting model training  [USA]
# MAGIC Predictive-analytics training that serves the forecasting use cases (continuous site forecasting, 24-hour forecast snapshots,
# MAGIC forecasted peak detection). Stand-in: one regularised regression per meter fitted with `applyInPandas` over the whole history window
# MAGIC (daily harmonics + last-interval lag on active power). The customer's real model is unknown, so this is a **floor** on compute;
# MAGIC the wall-clock and DBUs scale with the training window (the benchmark holds `history_days`; production would use more weeks).

# COMMAND ----------
import common
import numpy as np
import pandas as pd
from pyspark.sql import functions as F, types as T

p = common.params(dbutils, {"market": "usa", "n_channels": 21})
b = common.Bench(spark, p, "forecast_training")
iv = common.t(p, "interval_readings")

COEFS = [f"b{i}" for i in range(8)]
out_schema = T.StructType([T.StructField("meter_id", T.LongType()), T.StructField("rmse", T.DoubleType()),
                           T.StructField("n_obs", T.LongType())] + [T.StructField(c, T.DoubleType()) for c in COEFS])


def train(pdf: pd.DataFrame) -> pd.DataFrame:
    pdf = pdf.sort_values("ts")
    y = pdf["c03"].to_numpy(dtype=float)
    h = (pdf["slot"].to_numpy(dtype=float) / 288.0) * 2 * np.pi
    X = np.column_stack([np.ones(len(y)), np.sin(h), np.cos(h), np.sin(2 * h), np.cos(2 * h), np.sin(3 * h), np.cos(3 * h), np.roll(y, 1)])
    X, y = X[1:], y[1:]
    beta = np.linalg.solve(X.T @ X + 1.0 * np.eye(X.shape[1]), X.T @ y)
    rmse = float(np.sqrt(np.mean((y - X @ beta) ** 2)))
    return pd.DataFrame([{"meter_id": int(pdf["meter_id"].iloc[0]), "rmse": rmse, "n_obs": int(len(y)),
                          **{c: float(v) for c, v in zip(COEFS, beta)}}])


# COMMAND ----------
with b.step("train_weekly", note=f"window_days={p['history_days']}") as s:
    d = (spark.table(iv).where("status = 'OK'")
         .select("meter_id", "ts", "c03", (F.hour("ts") * 12 + (F.minute("ts") / 5).cast("int")).alias("slot")))
    s["rows_in"] = d.count()
    d.groupBy("meter_id").applyInPandas(train, schema=out_schema).writeTo(common.t(p, "result_forecast_models")).createOrReplace()
    s["rows_out"] = spark.table(common.t(p, "result_forecast_models")).count()
