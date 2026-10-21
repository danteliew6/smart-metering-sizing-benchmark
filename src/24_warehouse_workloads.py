# Databricks notebook source
# MAGIC %md
# MAGIC # 24 - SQL warehouse workloads: reporting, interval lookup, BI serving, BI refresh
# MAGIC Runs query sets on serverless SQL warehouses of several sizes and records cold (first) and warm (median of 3) latency
# MAGIC per query. The sizing model prices these as *warehouse uptime hours*, so the output here informs (a) which warehouse
# MAGIC size keeps dashboards responsive at this data volume and (b) the daily refresh duration.
# MAGIC
# MAGIC `workload`: `register` (ANZ reporting on meters), `lookup` (ANZ interval usage chart per meter), `bi` (dashboard
# MAGIC query mix), `refresh` (daily gold refresh).

# COMMAND ----------
import statistics
import common

p = common.params(dbutils, {"workload": "bi", "sizes": "small,medium", "wh_xsmall": "", "wh_small": "", "wh_medium": ""})
wh = {"xsmall": p["wh_xsmall"], "small": p["wh_small"], "medium": p["wh_medium"]}
b = common.Bench(spark, p, f"warehouse_{p['workload']}")
iv, meters, conv = common.t(p, "interval_readings"), common.t(p, "meters"), common.t(p, "conversations")
day = common.data_days(spark, p)[-1]
first_day = common.data_days(spark, p)[0]
kw = dict(catalog=p["catalog"], schema=p["schema"])

QUERIES = {
    "register": {
        "meters_by_business_service": f"SELECT business_service, count(*) FROM {meters} GROUP BY 1",
        "meters_by_tenant_type": f"SELECT tenant_id, meter_type, count(*) FROM {meters} GROUP BY 1, 2",
        "cellular_firmware_mix": f"SELECT firmware, count(*) FROM {meters} WHERE comms_type = 'Cellular' GROUP BY 1",
        "fault_meters_old_firmware": f"SELECT count(*) FROM {meters} WHERE status = 'FAULT' AND firmware < '2.0.0'",
        "tenant_status_matrix": f"SELECT tenant_id, status, count(DISTINCT meter_id) FROM {meters} GROUP BY 1, 2",
    },
    "lookup": {f"meter_{m}_7d": f"SELECT ts, c01, c03 FROM {iv} WHERE meter_id = {m} AND reading_date >= '{first_day}' ORDER BY ts"
               for m in (101, 2503, 17011, 33333, 50007, 64999, 78234, 99001)},
    "bi": {
        "kpi_read_success": f"SELECT count_if(status = 'OK') / count(*) FROM {iv} WHERE reading_date = '{day}'",
        "read_success_by_hour": f"SELECT hour(ts) h, count_if(status = 'OK') / count(*) FROM {iv} WHERE reading_date = '{day}' GROUP BY 1 ORDER BY 1",
        "voltage_by_tenant": f"SELECT m.tenant_id, avg(i.c01), max(i.c01) FROM {iv} i JOIN {meters} m USING (meter_id) WHERE i.reading_date = '{day}' GROUP BY 1",
        "comms_failures_by_reason": f"SELECT failure_reason, count(*) FROM {conv} WHERE reading_date = '{day}' AND outcome = 'FAILED' GROUP BY 1",
        "top_meters_by_load": f"SELECT meter_id, sum(c03) kwh5 FROM {iv} WHERE reading_date = '{day}' GROUP BY 1 ORDER BY 2 DESC LIMIT 50",
        "meter_register_filter": f"SELECT count(*) FROM {meters} WHERE business_service = 'Solar' AND status = 'ACTIVE'",
    },
    "refresh": {
        "gold_daily_meter_summary": f"""CREATE OR REPLACE TABLE {common.t(p, 'gold_daily_meter_summary')} AS
            SELECT meter_id, reading_date, avg(c01) avg_voltage, max(c01) max_voltage, sum(c03) energy_kw5,
                   count_if(status = 'OK') ok_reads, count_if(status = 'FAILED') failed_reads
            FROM {iv} WHERE reading_date = '{day}' GROUP BY meter_id, reading_date""",
    },
}

# COMMAND ----------
queries = QUERIES[p["workload"]]
reps = 1 if p["workload"] == "refresh" else 3
for size in p["sizes"].split(","):
    if not wh.get(size):
        print(f"skip {size}: no warehouse id"); continue
    p_size = dict(p, variant=f"dbsql-serverless-{size}")
    bs = common.Bench(spark, p_size, f"warehouse_{p['workload']}")
    session = []
    for name, q in queries.items():
        times, n = [], None
        for _ in range(reps):
            secs, n = common.run_on_warehouse(wh[size], q, **kw)
            times.append(secs)
        session.append(statistics.median(times))
        bs.record(f"{name}:cold", times[0], rows_out=n)
        bs.record(f"{name}:warm_median", statistics.median(times), rows_out=n)
    bs.record("query_set_total_warm", sum(session), note=f"{len(queries)} queries")
