# Use cases -> notebooks -> model cost lines

## ANZ (9 customer use cases)
| Customer use case | Notebook / job | Model cost line (assumed compute) |
|---|---|---|
| Interval data ingestion | `10_ingest_interval` / `ingest_interval_anz_*` | triggered serverless every 15 min (alt.: always-on classic) |
| Conversation log ingestion | `11_ingest_conversation` / `ingest_conversation_anz_*` | triggered serverless every 15 min (alt.: always-on classic) |
| Meter & business service register sync | `20_register_sync` / `anz_register_sync` | daily serverless batch, 4 VMs |
| Reporting on meters | `24_warehouse_workloads` (register) / `anz_reporting_on_meters` | DBSQL X-Small, 2-3 h per workday |
| Poor comms performance | `21_poor_comms` / `anz_poor_comms` | daily serverless batch, 4 VMs |
| Billing aggregation | `22_billing` / `anz_billing` | daily serverless batch, 4 VMs |
| Reporting on interval data | `24_warehouse_workloads` (lookup) / `anz_reporting_on_interval_data` | DBSQL Small, 2-4 h per workday |
| Voltage excursions | `23_voltage_excursions` / `anz_voltage_excursions` | daily serverless batch, 4 VMs per 1M interval meters |
| BI dashboards & visuals | `24_warehouse_workloads` (bi, refresh) / `bi_serving_anz`, `bi_refresh_anz` | DBSQL Small/Medium/Large by tier, 3.5 h per workday + daily refresh |

## USA (24 customer use cases grouped into 15 cost lines)
| Cost line | Customer use cases covered | Notebook / job |
|---|---|---|
| Real-time streaming ingestion | Real-Time Streaming Ingestion | `10_ingest_interval` / `ingest_interval_usa_*` |
| Weather / 3rd-party ingestion | Weather data ingestion; 3rd party meter data ingestion | `12_ingest_weather_third_party` / `usa_weather_third_party` |
| Transformer & substation aggregation | Aggregation + load stress; loading alarms (W/A/C); substation aggregation | `30_transformer_aggregation` / `usa_transformer_aggregation[_classic]` |
| Transformer forecast aggregation | Loading alarms (Info); reverse power flow | `31_site_forecast` (tfa step) / `usa_site_forecast` |
| Peak detection actual / forecasted | Peak detection - actual; - forecasted | `32_peak_detection` / `usa_peak_detection` |
| Health monitoring | Voltage alarms; read performance & last read; comms-failure alarm | `33_health_monitoring` / `usa_health_monitoring` |
| Site forecasts | Continuous site forecasting; 24-hour forecast snapshots | `31_site_forecast` / `usa_site_forecast` |
| Estimation of missing data | Estimation of missing data | `34_missing_data_estimation` / `usa_missing_data` |
| EV / PV detection | EV detection, disaggregation, charging sessions; PV detection, disaggregation | `35_ev_pv_detection` / `usa_ev_pv_detection` |
| Outage detection | Outage/restoration detection & alarms | `36_outage_detection` / `usa_outage_detection` |
| BI serving / refresh | BI dashboards & visuals; arbitrary aggregation | `24_warehouse_workloads` / `bi_serving_usa`, `bi_refresh_usa` |
