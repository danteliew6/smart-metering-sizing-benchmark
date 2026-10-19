# Databricks notebook source
# MAGIC %md
# MAGIC # 22 - Billing aggregation (daily)  [ANZ]
# MAGIC Conversations per tenant per day split peak / off-peak (07:00-21:00 = peak) plus meters per business service per tenant.

# COMMAND ----------
import common

p = common.params(dbutils)
b = common.Bench(spark, p, "billing")
conv, meters = common.t(p, "conversations"), common.t(p, "meters")

# COMMAND ----------
with b.step("billing_aggregates", rows_in=spark.table(conv).count()) as s:
    spark.sql(f"""
      CREATE OR REPLACE TABLE {common.t(p, 'result_billing_conversations')} AS
      SELECT tenant_id, reading_date,
             sum(CASE WHEN hour(start_ts) BETWEEN 7 AND 20 THEN 1 ELSE 0 END) AS peak_conversations,
             sum(CASE WHEN hour(start_ts) BETWEEN 7 AND 20 THEN 0 ELSE 1 END) AS offpeak_conversations,
             count(*) AS conversations
      FROM {conv} GROUP BY tenant_id, reading_date""")
    spark.sql(f"""
      CREATE OR REPLACE TABLE {common.t(p, 'result_billing_meters')} AS
      SELECT tenant_id, business_service, count(*) AS meters FROM {meters} GROUP BY tenant_id, business_service""")
    s["rows_out"] = spark.table(common.t(p, "result_billing_conversations")).count()
