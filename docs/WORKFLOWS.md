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
| 1 | **Streaming core** | Real-Time Streaming Ingestion; Estimation of missing data; Transformer Aggregation & Load Stress Score; Substation Aggregation; Transformer Loading alarms (W/A/C); Voltage alarms | Continuous (5-min) | One shared always-on Classic cluster (switch `Shared`/`Separate`) |
| 2 | External data feeds | Weather data ingestion; 3rd party meter data ingestion | 4-hourly / daily | Serverless batch |
| 3 | Device health & comms | Read performance, BSU & last read; Communications Failure alarm | 5-min (flexible) / daily | Daily serverless batch |
| 4 | Transformer load analytics | Transformer Loading alarms (Info); Reverse Power Flow; Peak detection - actual | Daily / 4-hourly | Serverless batch |
| 5 | Forecasting chain | Weekly forecasting model training; Continuous site forecasting; 24 hour forecast snapshots; Peak detection - forecasted | Weekly / daily / 4-hourly | Serverless batch |
| 6 | DER detection (EV & PV) | EV Detection, Disaggregation, Charging sessions; PV Detection, Disaggregation | Daily (disaggregation continuous in file) | Serverless batch |
| 7 | Outage detection | Outage/restoration detection & alarms | Continuous in file | Serverless batch |
| 9 | **5-min micro-batch workflow** | Outage/restoration detection; EV Disaggregation; PV Disaggregation; Read performance & last read (5-min update); Continuous site forecasting (5-min nowcast correction) | Every 5 min | ONE serverless job run every 5 min with five tasks (start-up paid once) |
| 8 | BI & reporting | Arbitrary aggregation of voltage & load; BI dashboards (serving + daily refresh) | On demand / daily | DBSQL serverless |

Data flow: Streaming core -> {Device health, Transformer load analytics, Forecasting chain, DER detection, Outage detection} -> BI & reporting. Weekly training -> forecasts -> forecasted peak.

## What changed in the cost model
* **Streaming core is one cluster.** The three previously separate always-on clusters (ingestion, aggregation, missing-data estimation) are pooled:
  1 driver plus workers whose total vCPU equals the three separate clusters' worker vCPU (D8ds). Worker capacity is unchanged; the duplicate drivers and idle floors go away.
  This saves about $760/month (about $9.1k/yr) at every tier - roughly 5% of total at 1M meters, 18% at 1k meters. No extra capacity is assumed for the alarm queries.
* **Voltage alarms moved** from the daily health batch into the streaming core, closing a frequency gap (the file asks for continuous, not flexible).
* Every other workflow is a presentation grouping: the model sums its members (sequential tasks on equal-size clusters cost the same as separate jobs),
  except that a single job run pays serverless start-up once - which the benchmark below measures.

## 5-minute micro-batches (customer: continuous but flexible, 5 minutes will do)
The use cases that are continuous in the file (outage detection, EV/PV disaggregation, read performance, site-forecast refresh) were previously costed as daily
batches. They now run as incremental jobs every 5 minutes (`src/38_frequency_sweep.py`, job `usa_frequency_sweep`; fit with `scripts/fit_frequency_model.py`).
Run time is nearly flat from 200k to 4.9M rows read (about 6-10 s), so cost is dominated by the **fixed per-run start-up charge**, not by data volume - which is why they run as ONE shared job.
At 5 minutes that is about $894/month of start-up (288 runs/day x ~0.23 DBU, **provisional** until the billed fit lands) plus a few hundred dollars of variable cost at 1M meters.
Peak detection follows the customer file and has no continuous option: actual = daily, forecasted = 4-hourly.
The cost sheet has an execution switch for these five use cases: **Micro-batches** (default, one shared 5-minute job) or **Continuous** (always-on Classic cluster(s), sized by estimate: one shared cluster or five). At 1M meters continuous costs about $4.9k/month against about $1.5k for micro-batches.
Not costed: running these as streaming queries on the shared always-on cluster, which would avoid per-run start-up but needs a streaming test to size.

## Benchmark: `usa_end_to_end_workflow`
One job, 11 notebook tasks wired with the dependency edges above (ingest -> missing data -> transformer aggregation -> peak detection; ingest -> training -> forecast -> peak detection;
ingest -> health / EV&PV / outage; all -> BI refresh). All tasks succeeded. Billed DBUs for the job run are compared with the sum of the separate
per-use-case runs (`docs/results/`), which shows how much the fixed per-run serverless overhead (~0.2-0.25 DBU) shrinks when work shares one run.

> Note: an earlier run of this job (2026-10-21) read an empty day because notebooks derived the latest day from today's date; they now use `common.data_days()` (the days actually in the table). Re-run before relying on its billing.
