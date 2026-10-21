# Databricks notebook source
# MAGIC %md
# MAGIC # 90 - Model vs measured report
# MAGIC Joins `bench_results` (runtime per step) with `system.billing.usage` (billed DBUs per job run) and with
# MAGIC `docs/model_assumptions.csv` (what the sizing models assume). Writes `<catalog>.<schema_anz|usa>.report_model_vs_measured`.
# MAGIC
# MAGIC *Billing lag:* system tables can trail the run by several hours - re-run this notebook later if `billed_dbu` is null.
# MAGIC *Warehouse jobs:* DBUs on `warehouse_*` jobs belong to the driver notebook, not the SQL warehouse, so they are not compared.
# MAGIC *Extrapolation:* measured DBU x (model scale / benchmark scale) assumes cost scales linearly with meters. That
# MAGIC over-states fixed overheads (cluster start-up) and under-states shuffle growth; read ratios as +/-50%, not exact.

# COMMAND ----------
import os
import pandas as pd
from pyspark.sql import functions as F, types as T

dbutils.widgets.text("catalog", "dante_classic_stable_catalog")
dbutils.widgets.text("schema_anz", "sm_bench_anz")
dbutils.widgets.text("schema_usa", "sm_bench_usa")
dbutils.widgets.text("serverless_mult", "1.0")      # keep equal to the cost sheets (Inputs: serverless multiplier); 2.34 = cost-calculator rating
catalog, schemas = dbutils.widgets.get("catalog"), {"anz": dbutils.widgets.get("schema_anz"), "usa": dbutils.widgets.get("schema_usa")}
SERVERLESS_MULT = float(dbutils.widgets.get("serverless_mult"))   # serverless-node DBU rating vs classic used by the cost sheets (see docs/RESULTS.md section 1b)
DBU_RATE = {"jobs_serverless": 0.50, "jobs_classic": 0.30}     # $/DBU used by the Azure Australia East model

here = os.getcwd()
assumptions = pd.read_csv(os.path.join(here, "..", "docs", "model_assumptions.csv"), dtype={"batches": str})

# COMMAND ----------
for market, schema in schemas.items():
    res = spark.table(f"{catalog}.{schema}.bench_results").where(F.col("market") == market)
    # latest run per (use_case, variant, step)
    latest = (res.withColumn("rn", F.row_number().over(__import__("pyspark.sql.window", fromlist=["Window"]).Window
              .partitionBy("use_case", "variant", "step", "note").orderBy(F.col("recorded_at").desc()))).where("rn = 1").drop("rn"))
    pdf = latest.toPandas()
    pdf["batches"] = pdf.note.str.extract(r"batches=(\d+)")[0].fillna("")
    if pdf.empty:
        print(f"no results for {market}"); continue
    runs = ",".join(f"'{r}'" for r in pdf.job_run_id.dropna().unique())
    bill = spark.sql(f"""SELECT usage_metadata.job_run_id AS job_run_id, sum(usage_quantity) AS billed_dbu,
                                 first(sku_name) AS sku_name
                         FROM system.billing.usage WHERE usage_metadata.job_run_id IN ({runs}) GROUP BY 1""").toPandas() if runs else pd.DataFrame()
    pdf = pdf.merge(bill, on="job_run_id", how="left")
    pdf["billed_dbu"] = pd.to_numeric(pdf.billed_dbu, errors="coerce").astype(float)
    pdf["job_seconds"] = pdf.groupby("job_run_id").seconds.transform("sum")
    pdf["billed_dbu_alloc"] = pdf.billed_dbu * pdf.seconds / pdf.job_seconds          # split a job's DBUs across its steps
    pdf["rows_per_second"] = pdf.rows_out / pdf.seconds
    sub = assumptions[assumptions.market == market]
    sub = sub.assign(batches=sub.batches.fillna("").astype(str))
    rep = sub.merge(pdf.rename(columns={"variant": "variant_run"}), left_on=["use_case", "notebook_step", "variant", "batches"],
                    right_on=["use_case", "step", "variant_run", "batches"], how="left", suffixes=("_model", ""))
    rep["scale_factor"] = rep.scale_meters / rep.benchmark_meters
    rep["measured_seconds_benchmark"] = rep.seconds
    # Serverless runs carry a fixed per-run DBU overhead (start-up) that does not scale with data. Estimate it from the
    # serverless interval-ingestion runs (1 batch vs 8 batches): fixed = dbu_1 - marginal_dbu_per_row x rows_1.
    ing = pdf[(pdf.use_case == "interval_ingestion") & (pdf.variant == "serverless")]
    a, c = ing[ing.batches == "1"], ing[ing.batches == "8"]
    fixed = 0.0
    if len(a) and len(c) and a.billed_dbu.notna().all() and c.billed_dbu.notna().all():
        marg = (c.billed_dbu.iloc[0] - a.billed_dbu.iloc[0]) / (c.rows_out.iloc[0] - a.rows_out.iloc[0])
        fixed = max(a.billed_dbu.iloc[0] - marg * a.rows_out.iloc[0], 0.0)
    rep["fixed_dbu_per_run"] = fixed
    rep["variable_dbu"] = (rep.billed_dbu - fixed).clip(lower=0) * rep.seconds / rep.job_seconds
    rep["measured_dbu_at_model_scale"] = fixed + rep.variable_dbu * rep.scale_factor
    always_on = rep.minutes_per_run >= 1440                     # continuous lines are judged on throughput, not DBU per run
    rep.loc[always_on, "measured_dbu_at_model_scale"] = float("nan")
    rep["model_dbu_per_run"] = rep.model_nodes_at_scale * rep.node_equiv_dbu_per_hr * rep.minutes_per_run / 60 * \
        rep.sku.map(lambda s: SERVERLESS_MULT if s == "jobs_serverless" else 1.0)
    rep["measured_vs_model"] = rep.measured_dbu_at_model_scale / rep.model_dbu_per_run
    rep["effective_dbu_per_hr"] = rep.billed_dbu_alloc / (rep.seconds / 3600)
    rep = rep.drop(columns=["note_model", "note"], errors="ignore")
    NUM = {"model_nodes_at_scale", "node_equiv_dbu_per_hr", "minutes_per_run", "runs_per_day", "scale_meters", "benchmark_meters",
           "seconds", "rows_in", "rows_out", "billed_dbu", "job_seconds", "billed_dbu_alloc", "rows_per_second", "scale_factor",
           "measured_seconds_benchmark", "measured_dbu_at_model_scale", "fixed_dbu_per_run", "variable_dbu", "model_dbu_per_run", "measured_vs_model", "effective_dbu_per_hr"}
    out_schema = T.StructType([T.StructField(c, T.DoubleType() if c in NUM else T.StringType()) for c in rep.columns])
    rows = [tuple(None if pd.isna(v) else (float(v) if c in NUM else str(v)) for c, v in zip(rep.columns, r)) for r in rep.itertuples(index=False)]
    out = spark.createDataFrame(rows, out_schema)
    spark.sql(f"DROP TABLE IF EXISTS {catalog}.{schema}.report_model_vs_measured")
    out.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.report_model_vs_measured")
    print(market, len(rep), "rows")
    display(spark.table(f"{catalog}.{schema}.report_model_vs_measured"))
