#!/usr/bin/env python3
"""Fit the per-run cost model for the frequency sweep: for each use case,
    seconds = a + b * rows_read          (runtime)
    DBU     = F + d * rows_read          (billed DBUs, needs system.billing.usage - lags a few hours)
and write docs/results/frequency_fit.csv.

Usage: python3 scripts/fit_frequency_model.py --profile <cli-profile> --warehouse-id <id> [--catalog c] [--schema s]
"""
import argparse, csv, json, pathlib, subprocess

ap = argparse.ArgumentParser()
ap.add_argument("--profile", required=True); ap.add_argument("--warehouse-id", required=True)
ap.add_argument("--catalog", default="dante_classic_stable_catalog"); ap.add_argument("--schema", default="sm_bench_usa")
a = ap.parse_args()


def query(stmt):
    body = {"warehouse_id": a.warehouse_id, "statement": stmt, "wait_timeout": "50s"}
    r = json.loads(subprocess.run(["databricks", "api", "post", "/api/2.0/sql/statements", "-p", a.profile, "--json", json.dumps(body)],
                                  capture_output=True, text=True, check=True).stdout)
    if r["status"]["state"] != "SUCCEEDED":
        raise SystemExit(r["status"])
    return r.get("result", {}).get("data_array", [])


rows = query(f"""
WITH r AS (SELECT use_case, job_run_id, seconds, rows_in, row_number() OVER (PARTITION BY use_case, note ORDER BY recorded_at DESC) rn
           FROM {a.catalog}.{a.schema}.bench_results WHERE use_case LIKE 'freq_%' AND rows_in > 0),
b AS (SELECT usage_metadata.job_run_id jr, sum(usage_quantity) dbu FROM system.billing.usage WHERE usage_date >= current_date() - 7 GROUP BY 1)
SELECT r.use_case, r.rows_in, r.seconds, b.dbu FROM r LEFT JOIN b ON b.jr = r.job_run_id WHERE r.rn = 1 ORDER BY 1, 2""")


def ols(xs, ys):
    n = len(xs); mx, my = sum(xs) / n, sum(ys) / n
    sxx = sum((x - mx) ** 2 for x in xs)
    slope = sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / sxx if sxx else 0.0
    return my - slope * mx, slope


out, by = [], {}
for uc, rin, sec, dbu in rows:
    by.setdefault(uc, []).append((float(rin), float(sec), None if dbu is None else float(dbu)))
for uc, pts in sorted(by.items()):
    a_s, b_s = ols([p[0] for p in pts], [p[1] for p in pts])
    billed = [p for p in pts if p[2] is not None]
    F, d = ols([p[0] for p in billed], [p[2] for p in billed]) if len(billed) >= 3 else (None, None)
    out.append({"use_case": uc, "points": len(pts), "a_seconds": round(a_s, 3), "b_seconds_per_row": b_s,
                "billed_points": len(billed), "F_dbu_per_run": None if F is None else round(F, 4), "d_dbu_per_row": d})
    print(out[-1])
path = pathlib.Path(__file__).resolve().parents[1] / "docs" / "results" / "frequency_fit.csv"
with open(path, "w", newline="") as f:
    w = csv.DictWriter(f, fieldnames=list(out[0].keys())); w.writeheader(); w.writerows(out)
print("wrote", path)
