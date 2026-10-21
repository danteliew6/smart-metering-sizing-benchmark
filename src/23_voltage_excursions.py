# Databricks notebook source
# MAGIC %md
# MAGIC # 23 - Voltage excursions (daily)  [ANZ]  /  Voltage alarms (health monitoring)  [USA]
# MAGIC Per meter, per day: intervals outside the +/-10% band x interval length = minutes out of band; list meters over a threshold.

# COMMAND ----------
import common

p = common.params(dbutils)
b = common.Bench(spark, p, "voltage_excursions")
iv = common.t(p, "interval_readings")
day = common.data_days(spark, p)[-1]
lo, hi = common.NOMINAL_V * (1 - common.BAND), common.NOMINAL_V * (1 + common.BAND)

# COMMAND ----------
with b.step("excursions_one_day") as s:
    s["rows_in"] = spark.table(iv).where(f"reading_date = '{day}'").count()
    spark.sql(f"""
      CREATE OR REPLACE TABLE {common.t(p, 'result_voltage_excursions')} AS
      SELECT meter_id, reading_date,
             sum(CASE WHEN c01 < {lo} OR c01 > {hi} THEN 5 ELSE 0 END) AS minutes_out_of_band,
             max(c01) AS max_voltage, min(c01) AS min_voltage
      FROM {iv} WHERE reading_date = '{day}' AND status = 'OK'
      GROUP BY meter_id, reading_date HAVING sum(CASE WHEN c01 < {lo} OR c01 > {hi} THEN 5 ELSE 0 END) >= 30""")
    s["rows_out"] = spark.table(common.t(p, "result_voltage_excursions")).count()
