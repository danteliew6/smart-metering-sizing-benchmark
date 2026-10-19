# Databricks notebook source
# MAGIC %md
# MAGIC # 21 - Poor comms performance: find, rank, categorise (daily)  [ANZ]
# MAGIC Meters with no successful read in 7 days, ranked by days since last read and failure rate, failures categorised.

# COMMAND ----------
import common

p = common.params(dbutils)
b = common.Bench(spark, p, "poor_comms")
conv = common.t(p, "conversations")
out = common.t(p, "result_poor_comms")

# COMMAND ----------
with b.step("rank_and_categorise", rows_in=spark.table(conv).count()) as s:
    spark.sql(f"""
      CREATE OR REPLACE TABLE {out} AS
      WITH agg AS (
        SELECT meter_id, tenant_id,
               max(CASE WHEN outcome = 'SUCCESS' THEN start_ts END) AS last_success_ts,
               count(*) AS conversations, sum(CASE WHEN outcome = 'FAILED' THEN 1 ELSE 0 END) AS failures,
               mode(failure_reason) AS top_failure_reason
        FROM {conv} WHERE reading_date >= date_sub(current_date(), 7) GROUP BY meter_id, tenant_id)
      SELECT *, datediff(current_timestamp(), last_success_ts) AS days_since_last_read,
             failures / conversations AS failure_rate,
             CASE WHEN last_success_ts IS NULL THEN 'NO_READ_7D'
                  WHEN failures / conversations > 0.5 THEN 'CHRONIC' WHEN failures / conversations > 0.2 THEN 'INTERMITTENT'
                  ELSE 'HEALTHY' END AS category,
             rank() OVER (ORDER BY (last_success_ts IS NULL) DESC, failures / conversations DESC) AS poor_rank
      FROM agg WHERE last_success_ts IS NULL OR failures / conversations > 0.2""")
    s["rows_out"] = spark.table(out).count()
