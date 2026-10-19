# Results - reference run (2026-10-19)

Run on AWS us-east-1, 100,000 interval meters, 7 days of history, serverless jobs compute and classic `m5d.xlarge` clusters,
serverless SQL warehouses (Small / Medium). ANZ also uses 600,000 platform meters (3.6M conversations/day).
All 30 benchmark jobs succeeded. Raw timings: `docs/results/*_bench_results.csv`.

> **Status: runtimes, throughput and storage are measured. Billed DBUs are NOT yet in this file.** `system.billing.usage` lagged the runs by
> ~2 hours when this was written. Run `databricks bundle run report` once it lands (`billed_dbu`, `measured_vs_model` and
> `effective_dbu_per_hr` in `report_model_vs_measured` fill in), then `python3 scripts/export_results.py` to refresh the CSVs.
> Until then **nothing below settles the serverless DBU rating (1.0x vs 2.34x)** - that needs billed DBUs.

## 1. Interval ingestion - minutes per run and cluster counts
| Compute | Rows | Wall-clock | Rows/second |
|---|---|---|---|
| Serverless, 1 batch (15 min, 100k meters) | 300,000 | 28.3 s | 10.6k |
| Serverless, 8-batch backlog | 2,400,000 | 39.0 s | 61.6k |
| Classic 1 driver + 1 worker (m5d.xlarge), 8-batch backlog | 2,400,000 | 93.6 s | 25.6k |
| Classic 1 driver + 3 workers, 8-batch backlog | 2,400,000 | 98.5 s | 24.4k |

(USA, 21 channels: serverless 29.1 s / 40.9 s; classic 1+1 113.6 s, 1+3 93.8 s.)

* **Serverless triggered run**: about 27 s fixed + 5.1 us/row. A 15-minute batch has `meters x 3` rows, so projected run times are
  0.6 min at 0.5M meters, 0.7 at 1M, 1.0 at 2M, 1.5 at 4M and **2.0 at 6M**. The model's 3-minute placeholder is conservative
  at every tier on wall-clock, assuming serverless scales out linearly. Whether it is conservative on *DBUs* depends on the billed rate (pending).
* **Always-on cluster**: required throughput is `meters x 288 / 86,400` = 3.3k rows/s at 1M and 20k rows/s at 6M. A classic 1+1 cluster
  drained a backlog at 21-26k rows/s, and 1+3 was no faster - so the drain was not CPU-bound and the limit is unknown. The model's
  "1 driver + 4 workers per 1M meters" (5 VMs at 1M, 25 at 6M) looks generous, but **do not cut cluster counts on this evidence alone**: the test
  was an `availableNow` drain of only 64 small files at ~446 bytes/row JSON (the original sizing assumed ~0.7 KB). A steady-state streaming test
  with a larger backlog (>=50M rows) is needed first.

## 2. Conversation ingestion
Needs 417 rows/s at 6M platform meters. One triggered serverless batch (37,500 rows) took 24.7 s; classic 1+1 drained 300k rows at 4.4k rows/s
(~10x the requirement, start-up dominated). The 2-VM floor is more than sufficient on throughput; a triggered run is ~25 s against the model's 2-minute placeholder.

## 3. Daily batch use cases (serverless wall-clock)
Linear projection to the model's tier: x10 for 100k -> 1M interval meters or 600k -> 6M platform meters. These are **Spark-native stand-ins**
for the customer's algorithms (see `METHODOLOGY.md`), so they are a floor, not a prediction.

| Use case | Measured | Projected at model tier | Model assumes |
|---|---|---|---|
| ANZ register sync (MERGE, 600k rows) | 11.1 s | 1.9 min (6M) | 7.5 min |
| ANZ poor comms (7 d, 25.2M rows) | 11.6 s | 1.9 min (6M) | 45 min |
| ANZ billing aggregates | 12.7 s | 2.1 min (6M) | 7.5 min |
| ANZ voltage excursions (1 d, 28.8M rows) | 13.4 s | 2.2 min (1M) / 13.4 min (6M) | 45 min per 1M |
| USA transformer forecast aggregation | 6.7 s | 1.1 min | 32.5 min |
| USA peak detection - actual (7 d) | 23.8 s | 4.0 min | 22.5 min |
| USA peak detection - forecasted | 4.8 s | 0.8 min | 22.5 min x 6/day |
| USA health monitoring (24 h) | 17.0 s | 2.8 min | 45 min |
| USA site forecasts (1 day ahead, 5-min) | 34.3 s | 5.7 min | 100 min |
| USA EV detection / PV detection | 21.6 s / 7.4 s | 3.6 / 1.2 min | 47.5 min each |
| USA outage detection (24 h) | 17.3 s | 2.9 min | 120 min |

The model's minutes are roughly 4-40x the projected data-handling time. Most of that gap is plausibly the real ML/algorithm cost (forecasting,
EV/PV and topology models), which these stand-ins do not include - so the right reading is "data handling is cheap; the model's
minutes are driven by algorithm cost the customer must supply", not "the model is 10x too high".

## 4. Transformer aggregation: always-on or triggered?
The USA model prices transformer + substation aggregation as an always-on cluster (9 x D8ds at 1M). One 5-minute slot aggregated in **5.2 s serverless /
8.3 s classic (m5d.2xlarge 1+1)** at 100k meters - a 1.7-2.8% duty cycle of the 300 s window (upper bound 17-28% if slot time scaled
linearly to 1M, which fixed overhead makes pessimistic). A full-day reprocess took 20.7 s serverless. Caveat: these timings exclude job start-up (not measured here; serverless job launch is typically tens of seconds), so a 5-minute job cadence may not be practical; the realistic alternatives are a small always-on streaming cluster or a
serverless streaming job - both far below 9 large VMs. Outage detection shows the same pattern: a 1-hour working set took 5.5 s.

## 5. Storage per row (tests the 35 bytes/row assumption)
| Table | Bytes/row (Delta) | Model |
|---|---|---|
| ANZ interval readings, 16 channels | **118.6** | 35 |
| USA interval readings, 21 channels | **155.3** | 35 |
| Conversation log | 13.1 | 25 |
| Landing JSON, interval | 446 | - |

The interval figure is 3.4x (ANZ) to 4.4x (USA) the model's. The generator's channel values are random doubles, which compress poorly, so
real data may land lower - but the original sizing's own test measured ~174 bytes/row, so **a storage multiplier of about 3x is better supported than 1.0**.
For the ANZ 6M tier that moves storage from about $0.9k to $3.0k per month (~+$25k/yr); for USA at 1M meters it is negligible.

## 6. BI / reporting warehouses
Warm latency was 0.5-0.9 s for every query on every warehouse size (X-Small, Small, Medium) at this data volume, so **warehouse size cannot be
discriminated at 100k meters**; repeat at 1M+ meters before choosing sizes. The daily gold refresh over 28.8M rows took 5.2 s (Small) / 6.4 s (Medium) ANZ and 2.9 / 3.3 s USA.
The model's driver for BI is warehouse *uptime hours* (3.5 h/workday), which this benchmark cannot measure.

## What this does and does not show
* Supported: serverless batch wall-clock is small relative to the model's minutes; the 3-minute interval-ingestion placeholder holds at every tier;
  conversation ingestion does not need an always-on cluster on throughput grounds; storage per row is understated by ~3-4x.
* Not yet supported: any cost/DBU conclusion. The serverless rating (1.0x vs 2.34x) and the DBU cost of each job need billed DBUs.
* Not tested: real ML algorithms, continuous-streaming steady state, scale above 100k meters, Azure compute and Azure rates.
