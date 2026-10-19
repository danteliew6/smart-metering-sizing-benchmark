#!/usr/bin/env python3
"""Export bench_results (and, when billing has landed, report_model_vs_measured) to docs/results/*.csv.

Usage: python3 scripts/export_results.py --profile <cli-profile> --warehouse-id <id> [--catalog c] [--schema-anz s] [--schema-usa s]
"""
import argparse, csv, json, pathlib, subprocess

ap = argparse.ArgumentParser()
ap.add_argument("--profile", required=True); ap.add_argument("--warehouse-id", required=True)
ap.add_argument("--catalog", default="dante_classic_stable_catalog")
ap.add_argument("--schema-anz", default="sm_bench_anz"); ap.add_argument("--schema-usa", default="sm_bench_usa")
a = ap.parse_args()
out = pathlib.Path(__file__).resolve().parents[1] / "docs" / "results"


def query(stmt):
    body = {"warehouse_id": a.warehouse_id, "statement": stmt, "wait_timeout": "50s"}
    r = json.loads(subprocess.run(["databricks", "api", "post", "/api/2.0/sql/statements", "-p", a.profile, "--json", json.dumps(body)],
                                  capture_output=True, text=True, check=True).stdout)
    if r["status"]["state"] != "SUCCEEDED":
        raise SystemExit(r["status"])
    return [c["name"] for c in r["manifest"]["schema"]["columns"]], r.get("result", {}).get("data_array", [])


for market, schema in (("anz", a.schema_anz), ("usa", a.schema_usa)):
    for table, order in (("bench_results", "recorded_at"), ("report_model_vs_measured", "use_case")):
        cols, rows = query(f"SELECT * FROM {a.catalog}.{schema}.{table} ORDER BY {order}")
        with open(out / f"{market}_{table}.csv", "w", newline="") as f:
            w = csv.writer(f); w.writerow(cols); w.writerows(rows)
        print(market, table, len(rows), "rows")
