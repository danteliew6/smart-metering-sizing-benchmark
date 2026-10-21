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
| 8 | BI & reporting | Arbitrary aggregation of voltage & load; BI dashboards (serving + daily refresh) | On demand / daily | DBSQL serverless |

Data flow: Streaming core -> {Device health, Transformer load analytics, Forecasting chain, DER detection, Outage detection} -> BI & reporting. Weekly training -> forecasts -> forecasted peak.

## What changed in the cost model
* **Streaming core is one cluster.** The three previously separate always-on clusters (ingestion, aggregation, missing-data estimation) are pooled:
  1 driver plus workers whose total vCPU equals the three separate clusters' worker vCPU (D8ds). Worker capacity is unchanged; the duplicate drivers and idle floors go away.
  This saves about $760/month (about $9.1k/yr) at every tier - roughly 5% of total at 1M meters, 18% at 1k meters. No extra capacity is assumed for the alarm queries.
* **Voltage alarms moved** from the daily health batch into the streaming core, closing a frequency gap (the file asks for continuous, not flexible).
* Every other workflow is a presentation grouping: the model sums its members (sequential tasks on equal-size clusters cost the same as separate jobs),
  except that a single job run pays serverless start-up once - which the benchmark below measures.

## Benchmark: `usa_end_to_end_workflow`
One job, 11 notebook tasks wired with the dependency edges above (ingest -> missing data -> transformer aggregation -> peak detection; ingest -> training -> forecast -> peak detection;
ingest -> health / EV&PV / outage; all -> BI refresh). All tasks succeeded. Billed DBUs for the job run are compared with the sum of the separate
per-use-case runs (`docs/results/`), which shows how much the fixed per-run serverless overhead (~0.2-0.25 DBU) shrinks when work shares one run.
