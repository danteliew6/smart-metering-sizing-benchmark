#!/usr/bin/env python3
"""Generate resources/jobs.yml - one job per benchmark so serverless DBUs can be attributed per run.

Edit the JOBS table below and re-run:  python3 scripts/gen_jobs.py > resources/jobs.yml
"""
import sys

MARKET = {
    "anz": dict(schema="${var.schema_anz}", platform="${var.n_platform_meters_anz}", channels="16"),
    "usa": dict(schema="${var.schema_usa}", platform="${var.n_interval_meters}", channels="21"),   # USA tiers are total meters
}
CLASSIC = {   # label -> (node type, workers); m5d/i3 are the AWS equivalents of the model's Azure D-/L-series VMs
    "classic-m5d.xlarge-1+1": ("m5d.xlarge", 1),
    "classic-m5d.xlarge-1+3": ("m5d.xlarge", 3),
    "classic-m5d.2xlarge-1+1": ("m5d.2xlarge", 1),
}
WAREHOUSES = '"${resources.sql_warehouses.wh_xsmall.id}"'

J = []
for m in ("anz", "usa"):
    J += [(f"gen_data_{m}", m, "00_generate_data", "serverless", {}),
          (f"land_files_{m}", m, "05_land_files", "serverless", {"n_batches": "8"}),
          (f"ingest_interval_{m}_serverless_1batch", m, "10_ingest_interval", "serverless", {"batches": "1"}),
          (f"ingest_interval_{m}_serverless_backlog", m, "10_ingest_interval", "serverless", {"batches": "8"}),
          (f"ingest_interval_{m}_classic_1p1_backlog", m, "10_ingest_interval", "classic-m5d.xlarge-1+1", {"batches": "8"}),
          (f"ingest_interval_{m}_classic_1p3_backlog", m, "10_ingest_interval", "classic-m5d.xlarge-1+3", {"batches": "8"}),
          (f"bi_serving_{m}", m, "24_warehouse_workloads", "serverless", {"workload": "bi", "sizes": "small,medium"}),
          (f"bi_refresh_{m}", m, "24_warehouse_workloads", "serverless", {"workload": "refresh", "sizes": "small,medium"})]
J += [
    ("ingest_conversation_anz_serverless_1batch", "anz", "11_ingest_conversation", "serverless", {"batches": "1"}),
    ("ingest_conversation_anz_serverless_backlog", "anz", "11_ingest_conversation", "serverless", {"batches": "8"}),
    ("ingest_conversation_anz_classic_1p1_backlog", "anz", "11_ingest_conversation", "classic-m5d.xlarge-1+1", {"batches": "8"}),
    ("anz_register_sync", "anz", "20_register_sync", "serverless", {}),
    ("anz_poor_comms", "anz", "21_poor_comms", "serverless", {}),
    ("anz_billing", "anz", "22_billing", "serverless", {}),
    ("anz_voltage_excursions", "anz", "23_voltage_excursions", "serverless", {}),
    ("anz_reporting_on_meters", "anz", "24_warehouse_workloads", "serverless", {"workload": "register", "sizes": "xsmall,small"}),
    ("anz_reporting_on_interval_data", "anz", "24_warehouse_workloads", "serverless", {"workload": "lookup", "sizes": "small"}),
    ("usa_weather_third_party", "usa", "12_ingest_weather_third_party", "serverless", {}),
    ("usa_transformer_aggregation", "usa", "30_transformer_aggregation", "serverless", {}),
    ("usa_transformer_aggregation_classic", "usa", "30_transformer_aggregation", "classic-m5d.2xlarge-1+1", {}),
    ("usa_site_forecast", "usa", "31_site_forecast", "serverless", {}),
    ("usa_forecast_training", "usa", "37_forecast_training", "serverless", {}),
    ("usa_peak_detection", "usa", "32_peak_detection", "serverless", {}),
    ("usa_health_monitoring", "usa", "33_health_monitoring", "serverless", {}),
    ("usa_missing_data", "usa", "34_missing_data_estimation", "serverless", {}),
    ("usa_ev_pv_detection", "usa", "35_ev_pv_detection", "serverless", {}),
    ("usa_outage_detection", "usa", "36_outage_detection", "serverless", {}),
]
ORDER = {  # orchestrators: jobs run one after another so each gets its own billing record
    "anz": [k for k, m, *_ in J if m == "anz" and not k.startswith(("gen_data", "land_files"))],
    "usa": ["usa_site_forecast"] + [k for k, m, *_ in J if m == "usa" and not k.startswith(("gen_data", "land_files")) and k != "usa_site_forecast"],
}


def job_yaml(key, m, nb, variant, extra):
    mk = MARKET[m]
    params = {"catalog": "${var.catalog}", "schema": mk["schema"], "market": m, "variant": variant,
              "n_interval_meters": "${var.n_interval_meters}", "n_platform_meters": mk["platform"], "n_channels": mk["channels"],
              "history_days": "${var.history_days}", "job_id": "{{job.id}}", "job_run_id": "{{job.run_id}}", **extra}
    if nb == "24_warehouse_workloads":
        params.update(wh_xsmall="${resources.sql_warehouses.wh_xsmall.id}", wh_small="${resources.sql_warehouses.wh_small.id}",
                      wh_medium="${resources.sql_warehouses.wh_medium.id}")
    out = [f"    {key}:", f"      name: sm-bench-{key.replace('_', '-')}", "      max_concurrent_runs: 1",
           "      tasks:", f"        - task_key: {key}", "          notebook_task:", f"            notebook_path: ../src/{nb}.py",
           "            base_parameters:"]
    out += [f"              {k}: \"{v}\"" for k, v in params.items()]
    if variant in CLASSIC:
        node, workers = CLASSIC[variant]
        out += ["          job_cluster_key: cluster", "      job_clusters:", "        - job_cluster_key: cluster", "          new_cluster:",
                "            spark_version: ${var.spark_version}", f"            node_type_id: {node}", f"            num_workers: {workers}",
                "            data_security_mode: SINGLE_USER", "            aws_attributes:", "              availability: ON_DEMAND",
                "              first_on_demand: 1"]
    return "\n".join(out)


def orchestrator(m):
    out = [f"    bench_all_{m}:", f"      name: sm-bench-all-{m}", "      max_concurrent_runs: 1", "      tasks:"]
    prev = None
    for key in ORDER[m]:
        out += [f"        - task_key: {key}", "          run_job_task:", f"            job_id: ${{resources.jobs.{key}.id}}"]
        if prev:
            out += ["          depends_on:", f"            - task_key: {prev}"]
        prev = key
    return "\n".join(out)


print("# GENERATED by scripts/gen_jobs.py - edit the script, not this file.")
print("resources:\n  jobs:")
for key, m, nb, variant, extra in J:
    print(job_yaml(key, m, nb, variant, extra)); print()
for m in ("anz", "usa"):
    print(orchestrator(m)); print()
# ----- end-to-end USA workflow: one job, tasks wired with the customer file's dependency column
WORKFLOW_TASKS = [   # (task key, notebook, depends on, extra params)
    ("ingest_interval", "10_ingest_interval", [], {"batches": "1"}),
    ("weather_third_party", "12_ingest_weather_third_party", [], {}),
    ("missing_data", "34_missing_data_estimation", ["ingest_interval"], {}),
    ("transformer_aggregation", "30_transformer_aggregation", ["missing_data"], {}),
    ("health_monitoring", "33_health_monitoring", ["ingest_interval"], {}),
    ("forecast_training", "37_forecast_training", ["ingest_interval"], {}),
    ("site_forecast", "31_site_forecast", ["forecast_training"], {}),
    ("peak_detection", "32_peak_detection", ["transformer_aggregation", "site_forecast"], {}),
    ("ev_pv_detection", "35_ev_pv_detection", ["ingest_interval"], {}),
    ("outage_detection", "36_outage_detection", ["ingest_interval"], {}),
    ("bi_refresh", "24_warehouse_workloads", ["peak_detection", "health_monitoring", "ev_pv_detection", "outage_detection", "weather_third_party"],
     {"workload": "refresh", "sizes": "small"}),
]


def workflow_job():
    mk = MARKET["usa"]
    out = ["    usa_end_to_end_workflow:", "      name: sm-bench-usa-end-to-end-workflow", "      max_concurrent_runs: 1", "      tasks:"]
    for key, nb, deps, extra in WORKFLOW_TASKS:
        params = {"catalog": "${var.catalog}", "schema": mk["schema"], "market": "usa", "variant": "serverless-workflow",
                  "n_interval_meters": "${var.n_interval_meters}", "n_platform_meters": mk["platform"], "n_channels": mk["channels"],
                  "history_days": "${var.history_days}", "job_id": "{{job.id}}", "job_run_id": "{{job.run_id}}", **extra}
        if nb == "24_warehouse_workloads":
            params.update(wh_xsmall="${resources.sql_warehouses.wh_xsmall.id}", wh_small="${resources.sql_warehouses.wh_small.id}",
                          wh_medium="${resources.sql_warehouses.wh_medium.id}")
        out += [f"        - task_key: {key}", "          notebook_task:", f"            notebook_path: ../src/{nb}.py", "            base_parameters:"]
        out += [f"              {k}: \"{v}\"" for k, v in params.items()]
        if deps:
            out += ["          depends_on:"] + [f"            - task_key: {d}" for d in deps]
    return "\n".join(out)


print(workflow_job()); print()

def sweep_job():
    mk = MARKET["usa"]
    params = {"catalog": "${var.catalog}", "schema": mk["schema"], "market": "usa", "variant": "serverless-sweep",
              "n_interval_meters": "${var.n_interval_meters}", "n_platform_meters": mk["platform"], "n_channels": mk["channels"],
              "history_days": "${var.history_days}", "job_id": "{{job.id}}", "job_run_id": "{{job.run_id}}",
              "use_case": "{{job.parameters.use_case}}", "window_minutes": "{{job.parameters.window_minutes}}"}
    out = ["    usa_frequency_sweep:", "      name: sm-bench-usa-frequency-sweep", "      max_concurrent_runs: 1", "      parameters:",
           "        - name: use_case", "          default: outage", "        - name: window_minutes", "          default: \"5\"",
           "      tasks:", "        - task_key: sweep", "          notebook_task:", "            notebook_path: ../src/38_frequency_sweep.py", "            base_parameters:"]
    out += [f"              {k}: \"{v}\"" for k, v in params.items()]
    return "\n".join(out)


print(sweep_job()); print()

print("""    report:
      name: sm-bench-report
      max_concurrent_runs: 1
      tasks:
        - task_key: report
          notebook_task:
            notebook_path: ../src/90_report.py
            base_parameters:
              catalog: "${var.catalog}"
              schema_anz: "${var.schema_anz}"
              schema_usa: "${var.schema_usa}"
""")
