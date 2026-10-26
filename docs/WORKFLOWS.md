# USA workflows - how the use cases depend on each other

The USA customer file has a **Dependency** column. Decoded (each number is a row of that tab):

| Use case | Depends on |
|---|---|
| Voltage alarms; Read performance & last read; Communications Failure alarm | Real-Time Streaming Ingestion |
| Transformer Aggregation & Load Stress Score | Estimation of missing data |
| Transformer Loading alarms (W/A/C and Info); Reverse Power Flow; Peak detection actual and forecasted; Substation Aggregation | Transformer Aggregation & Load Stress Score |
| Continuous site forecasting; 24 hour forecast snapshots | Real-Time Streaming Ingestion |
| EV Detection; PV Detection | Real-Time Streaming Ingestion |
| EV Disaggregation; EV Charging session times | EV Detection (and ingestion) |
| PV Disaggregation | PV Detection (and ingestion) |
| Real-Time Streaming Ingestion, Estimation of missing data, Weather, 3rd party, Outage detection, Arbitrary aggregation, BI | none stated |

Not stated in the file but assumed: missing-data estimation runs on ingested data; weekly forecasting model training (a customer clarification)
feeds the forecasting use cases; BI consumes every other workflow's outputs.

## Workflows
Use cases are combined when they depend on each other **and** run at compatible cadences. Continuous, daily and weekly work cannot share one job trigger or
one cluster, so one dependency chain can span several workflows (cross-workflow edges below).

| # | Workflow | Includes | Cadence | Compute in the cost model |
|---|---|---|---|---|
| 1 | **Streaming core** | Six workloads, listed below: Real-Time Streaming Ingestion; Estimation of missing data; Transformer Aggregation & Load Stress Score; Transformer Loading alarms (W/A/C); Substation Aggregation; Voltage alarms | Continuous (5-min) | One shared always-on Classic cluster (switch `Shared`/`Separate`) |
| 2 | External data feeds | Weather data ingestion; 3rd party meter data ingestion | 4-hourly / daily | Serverless batch |
| 3 | Device health & comms | Read performance, BSU & last read; Communications Failure alarm | 5-min (flexible) / daily | Daily serverless batch |
| 4 | Transformer load analytics | Transformer Loading alarms (Info); Reverse Power Flow; Peak detection - actual | Daily / 4-hourly | Serverless batch |
| 5 | Forecasting chain | Weekly forecasting model training; Continuous site forecasting; 24 hour forecast snapshots; Peak detection - forecasted | Weekly / daily / 4-hourly | Serverless batch |
| 6 | DER detection (EV & PV) | EV Detection, Charging sessions; PV Detection (daily); EV and PV Disaggregation run in the merged 5-min job (row 9) | Daily | Serverless batch |
| 7 | Outage detection | Outage/restoration detection & alarms | Continuous in file | Serverless batch |
| 9 | **5-min isolated serverless jobs** | Outage/restoration detection; EV & PV Disaggregation (one merged job); Read performance & last read (5-min update); Continuous site forecasting (5-min nowcast correction) | Every 5 min | Four separate serverless jobs, each run every 5 min (Inputs B14 switch: micro-batch or continuous 24/7) |
| 8 | BI & reporting | Arbitrary aggregation of voltage & load; BI dashboards (serving + daily refresh) | On demand / daily | DBSQL serverless |

Data flow: Streaming core -> {Device health, Transformer load analytics, Forecasting chain, DER detection, Outage detection} -> BI & reporting. Weekly training -> forecasts -> forecasted peak.

## Streaming core: what is on the shared cluster
In `Shared` mode these six customer use cases are all continuous and each feeds the next, so they run as streaming queries on one always-on cluster.
The Google Sheet's `Workload Breakdown` tab itemises them with the rationale and assumptions for combining them, and splits the cluster's size and cost by component.

| # | Customer use case | Component | Capacity in the model |
|---|---|---|---|
| 1 | Real-Time Streaming Ingestion | Ingestion | Sized explicitly |
| 2 | Estimation of missing data | Missing-data estimation | Sized explicitly |
| 3 | Transformer Aggregation & Load Stress Score | Aggregation & alarms | Sized explicitly |
| 4 | Transformer Loading alarms (Warning/Alarm/Critical) | Aggregation & alarms | Included, no extra capacity |
| 5 | Substation Aggregation | Aggregation & alarms | Included, no extra capacity |
| 6 | Voltage alarms | Ingestion stream query | Included, no extra capacity |

Cluster size = 1 driver + workers whose total vCPU equals the three components' standalone worker vCPU (D8ds). Not on the cluster: weather and 3rd-party ingestion, Info loading alarms, reverse power flow, peak detection, the four 5-minute jobs, EV/PV detection, training and BI.

## What changed in the cost model
* **Streaming core is one cluster.** The three previously separate always-on clusters (ingestion, aggregation, missing-data estimation) are pooled:
  1 driver plus workers whose total vCPU equals the three separate clusters' worker vCPU (D8ds). Worker capacity is unchanged; the duplicate drivers and idle floors go away.
  This saves about $760/month (about $9.1k/yr) at every tier - roughly 5% of total at 1M meters, 18% at 1k meters. No extra capacity is assumed for the alarm queries.
* **Voltage alarms moved** from the daily health batch into the streaming core, closing a frequency gap (the file asks for continuous, not flexible).
* Every other workflow is a presentation grouping: the model sums its members (sequential tasks on equal-size clusters cost the same as separate jobs),
  except that a single job run pays serverless start-up once - which the benchmark below measures.

## 5-minute micro-batches (customer: continuous but flexible, 5 minutes will do)
The use cases that are continuous in the file (outage detection, EV/PV disaggregation, read performance, site-forecast refresh) run either as 5-minute serverless micro-batches (default) or as 24/7 always-on Jobs Classic clusters with their own node estimates (separate from the serverless counts), chosen with the Inputs B14 switch. EV and PV disaggregation are merged into one job, since both work on small flagged subsets of the same reads.
Peak detection follows the customer file and has no continuous option: actual = daily, forecasted = 4-hourly.
Each of these four runs as its **own isolated serverless job**, triggered every 5 minutes. A triggered micro-batch does not run for the length of the interval: it starts, processes the last 5 minutes of data and stops, so it bills only for that short run. The cost sheet prices it like every other serverless line (VMs x minutes per run x DBU/hr x multiplier x $/DBU x runs per month) with an assumed **1 minute per run** (start-up plus processing; editable). That is about $130/month per job at 2 VMs and $518/month for the four at small tiers, rising to about $970/month at 1M meters where outage detection and forecast refresh need 5 VMs. Running every 15 minutes instead would cost about $324/month at 1M meters. The frequency-sweep notebook and fit script remain in the repo for measuring real per-run charges, but the cost model does not use them: the 1-minute run time is a logical assumption.

Not costed: running these as streaming queries on the shared always-on cluster, which would avoid per-run start-up but needs a streaming test to size.

## Benchmark: `usa_end_to_end_workflow`
One job, 11 notebook tasks wired with the dependency edges above (ingest -> missing data -> transformer aggregation -> peak detection; ingest -> training -> forecast -> peak detection;
ingest -> health / EV&PV / outage; all -> BI refresh). All tasks succeeded. Billed DBUs for the job run are compared with the sum of the separate
per-use-case runs (`docs/results/`), which shows how much the fixed per-run serverless overhead (~0.2-0.25 DBU) shrinks when work shares one run.

> Note: an earlier run of this job (2026-10-21) read an empty day because notebooks derived the latest day from today's date; they now use `common.data_days()` (the days actually in the table). Re-run before relying on its billing.
