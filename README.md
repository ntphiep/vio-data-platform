# vio-data-platform

Data platform for Vio Travel on Databricks. It pulls bookings, agencies, suppliers, users, travel ideas, payments
due, purchase attempts and accommodations from the Travel Compositor API (Vio's booking platform, operator `easia`),
keeps them in the Unity Catalog catalog `vio_travel` with their history, models them as a star schema with metric
views for reporting and Genie, shapes them for the Business Central API, and shows them in an AI/BI dashboard.

Research notes, field mapping and measurements live outside this repo, in `~/W/viotravel/vio_docs/`
(Vietnamese, starts at `README.md`).

## Flow

| Step | Code | Reads | Writes |
|---|---|---|---|
| `extract_tc` | `src/ingest_tc.py` | Travel Compositor API, GET only | volume `raw.landing`, gzip NDJSON, one file per page |
| `lakehouse` | pipeline `vio_travel_lakehouse`, `src/lakehouse/` | `raw.landing`, `ops.ingest_runs`, `ref.umbrella_agency` | `bronze.tc_*`, `silver.*`, `gold.*`, event log `ops.pipeline_event_log` |
| `bc_tables` | `src/bc_tables.py` | `silver.*`, `ref.*` | `business_central.*`, then a row in `ops.publish` |
| `data_quality` | `src/data_quality.py` | event log, `gold.*`, `ops.*` | `ops.dq_results`: expectations, `gold_` and `e2e_` checks |
| dashboard | `dashboards/vio_travel_overview.lvdash.json` | `gold.*`, `business_central.*`, `ops.*` | AI/BI dashboard Vio Travel overview, 5 pages |
| Genie | `genie/vio_travel_genie.geniespace.json` | `gold.*`, metric views | Genie space Vio Travel Genie |
| push | `local/push_to_bc.py` | `business_central.*` at the published versions | Business Central, company My Company; `ops.bc_sync` |
| alert | `resources/pipeline_freshness.alert.yml` | `ops.dq_results` | e-mail when no end-to-end check arrived for 75 minutes |

The four steps above the dashboard run in the job `vio_travel_pipeline` every 30 minutes, at minute 05 and 35
(Asia/Ho_Chi_Minh), as the service principal `vio-travel-pipeline`. Three more jobs: `vio_travel_catalog` reads the
accommodation catalog weekly, `vio_travel_backfill` re-extracts a date range month by month, and
`vio_travel_governance` applies tags, masks, grants and the metric views. The push into Business Central runs on a
local machine because it uses a delegated Azure CLI login, which a job cannot do; its systemd timer asks the Jobs
API every 5 minutes and pushes after each finished job run.

## Entities

| Entity | Endpoint | How it is read | Silver |
|---|---|---|---|
| bookings | `/booking/getBookings` | incremental by update date, 7-day windows, every run | `bookings` (SCD 1), `bookings_history` (SCD 2), `booking_services`, `booking_payments`, `booking_invoices`, `booking_price_components` |
| agencies | `/agency/{microsite}` | snapshot, every 6 hours | `agencies`, `agencies_history` |
| suppliers | `/suppliers` | snapshot, every 6 hours | `suppliers`, `suppliers_history` |
| users | `/user/{microsite}` | snapshot, every 6 hours | `users`, `users_history` |
| travel ideas | `/travelidea/{microsite}` | snapshot, every 6 hours | `travel_ideas`, `travel_ideas_history` |
| pending payments | `/pending-payments/{microsite}` | snapshot, every run | `pending_payments`, `pending_payments_history` |
| purchase attempts | `/purchase-attempts/{microsite}` | snapshot, every run | `purchase_attempts`, `purchase_attempts_history` |
| accommodations | `/accommodations` | snapshot of 283 pages, weekly, 4 parallel requests | `accommodations`, `accommodations_history` |
| accommodation datasheets | `/accommodations/{id}/datasheet` | booked hotels only, 200 per run, refreshed after 30 days | `accommodation_details` (SCD 1) |

`/microsites/{microsite}/manual-exchange-rates` answers 401 for Vio's API user, so it is not read. Contracts are
not read either; finance data comes from the pending payments, purchase attempts and the payment, invoice and
price breakdown blocks of each booking.

## Freshness

| Stage | Up to date within | Watched by |
|---|---|---|
| Travel Compositor API to bronze and silver | 30 minutes plus the run time | `e2e_extract_failing_or_stale` per entity, `e2e_pipeline_failed_or_stale` |
| silver to gold, the dashboard and Genie | same pipeline update | pipeline expectations, `gold_` checks |
| silver to `business_central` | same run | `e2e_business_central_behind_silver` |
| `business_central` to Business Central | 5 minutes after the run, 100 bookings per push | `e2e_push_stalled`, `e2e_sales_waiting_for_push` |
| the job itself | | SQL alert `pipeline_freshness`; anomaly detection of Unity Catalog on schemas `silver`, `gold`, `business_central` |

The Travel Compositor API filters bookings by update date only to the day (`ufrom`), so a run re-reads today and
yesterday. Webhooks would need a URL registered on Vio's Travel Compositor account, which is out of scope.

## Layers in catalog `vio_travel`

| Schema | Layer | What it holds |
|---|---|---|
| `raw` | landing | volume `landing`: API responses as returned, one folder per entity |
| `bronze` | raw history | `tc_<entity>`, one VARIANT row per landed record, append-only, never reset by a full refresh |
| `silver` | current state and history | see Entities; `*_history` tables are SCD type 2 with `__START_AT`, `__END_AT` |
| `gold` | analytics model | star schema, one big table and metric views, below |
| `business_central` | integration output | `customers`, `vendors`, `sales_invoices`, `sales_lines`, `purchase_invoices`, `purchase_lines` |
| `ref` | mappings | `umbrella_agency`, `customer_map`, `vendor_map`, maintained by Vio Finance |
| `governance` | governance | function `mask_pii` |
| `ops` | operations | `api_calls`, `ingest_runs`, `watermark`, `accommodation_fetch`, `pipeline_event_log`, `build_state`, `dq_results`, `publish`, `bc_sync` |

## Data model (schema `gold`)

Kimball star schema. Dimensions are type 1 (current values) with key `-1` for Unknown; `dim_agency_history`
gives the type 2 view for as-of questions. Every table is a materialized view of the pipeline with liquid
clustering and informational primary and foreign keys (`RELY`).

| Table | Grain | Keys and main columns |
|---|---|---|
| `dim_date` | one calendar day, 2015 to 2030 | `date_key` (yyyyMMdd), year, quarter, month, `year_month`, week, weekday |
| `dim_agency`, `dim_agency_history` | one agency, one agency version | `agency_key` (Travel Compositor agency ID), name, country, deferred terms, `is_umbrella` |
| `dim_supplier` | one supplier | `supplier_key`: `SP<id>` Travel Compositor supplier, `PV<code>` provider, `MN:<name>` manual service |
| `dim_service_type` | one service type | `service_type` (`hotelservice`, `manual_hotel`, ...), `service_category`, `is_manual` |
| `dim_user` | one microsite user | `user_key` (SHA-256 of `<agency ID>:<username>`, as usernames are often e-mail addresses), masked name and e-mail, profile, `b2c` |
| `dim_travel_idea` | one travel idea | `idea_key` (idea ID), title, trip type, price per person, destinations |
| `dim_accommodation` | one booked hotel | `accommodation_key` (`hotelId`), category, chain, destination, rating on a 10 scale |
| `fact_booking` | one booking, current state | keys to agency, user, idea, dates; status, trip type; `total_price`, `net_provider`, `gross_margin`, `pending_amount`, pax |
| `fact_booking_service` | one service of a booking | keys to booking, supplier, agency, service type, accommodation; sale and cost amounts |
| `fact_booking_payment` | one payment on a booking | first, second, additional or manual payment; amount, payment system |
| `fact_pending_payment` | one payment due | amount to pay, due date, `days_overdue`, `ageing_bucket` |
| `fact_booking_status_change` | one tracked change | previous and new status, amount change, from `silver.bookings_history` |
| `fact_purchase_attempt` | one checkout started | amount, trip type, `is_converted` when a booking carries the same idea ID |
| `obt_booking_service` | one service of a booking | `fact_booking_service` joined with every dimension, for ad-hoc SQL and Genie |
| `booking_metrics`, `service_metrics`, `receivable_metrics` | metric views | revenue, margin, cancellation rate; services by supplier; amounts due and overdue |

`SP` and `PV` keys of `supplier_key` equal the vendor numbers in `business_central.vendors`, unless Vio Finance
maps a provider to another vendor in `ref.vendor_map`.

## Reliability, performance and data quality

| Concern | How |
|---|---|
| API errors | up to 6 attempts on 429, 5xx, network errors and truncated JSON, full-jitter backoff capped at 120 s, `Retry-After` honoured, token renewed 5 minutes before expiry and shared by parallel readers |
| Missed records | distinct keys compared with `totalResults` for every booking window and snapshot; a short booking window is read again, then PARTIAL and the watermark does not move; a short snapshot is never applied |
| Exactly-once load | Auto Loader in the pipeline tracks every landed file; files are written as `.tmp` then renamed |
| Out-of-order updates | AUTO CDC orders booking versions by `lastUpdateDate`, then load time, so a replayed page never overwrites a newer version |
| History (SCD) | SCD type 2 by AUTO CDC FROM SNAPSHOT for the snapshot entities, which also closes keys that disappear (a paid pending payment); SCD type 2 of booking status and amounts; SCD type 1 for current state |
| Incremental transform | streaming tables process only new bronze rows; materialized views refresh incrementally on serverless when the plan allows; the 283,000-row accommodation catalog applies only changed rows |
| Layout | liquid clustering (`CLUSTER BY` on join keys, `CLUSTER BY AUTO` elsewhere) instead of partitions and Z-ORDER; deletion vectors, row tracking and change data feed on silver; predictive optimization on the catalog |
| Expectations | every layer; rows without a key are dropped, other rules warn; results copied from the event log into `ops.dq_results` |
| Checks | `ops.dq_results`; the check name starts with the layer it tests (`bronze_`, `silver_`, `gold_`, `bc_`, `e2e_`); block stops the publish of `business_central`, alert fails `data_quality` so the failure e-mail goes out |
| Consistent reads for the push | write-audit-publish: `bc_tables` records table versions in `ops.publish` only after its blocking checks pass, and `local/push_to_bc.py` reads `VERSION AS OF` those versions |
| Business Central writes | POST retried only on 429; documents matched by `externalDocumentNumber` and `vendorInvoiceNumber`; 3 failures with the same hash stop retries |
| Backfill and replay | job `vio_travel_backfill` (for-each over months, 3 at a time, watermark untouched); a failed or PARTIAL month fails its iteration, and "Repair run" re-runs only those months; job parameter `backfill_from`; a pipeline full refresh rebuilds silver and gold from bronze, which keeps every record; bronze and the snapshot history tables refuse a full refresh (`pipelines.reset.allowed` false) because their history exists only there |
| Retry and SLA | task retries with a minimum interval; pipeline e-mails on update and flow failure; job e-mail on failure and on runs over 30 minutes; SQL alert as dead-man switch |

## Governance

| Control | How |
|---|---|
| Identity | jobs and pipeline run as the service principal `vio-travel-pipeline`; the secret scope `vio-tc` is readable only by it and the owner |
| Column masks | `governance.mask_pii` on e-mail, phone, name and address columns of silver and `dim_user`; values show only to the group `vio_pii_readers` |
| Tags | table tag `contains_pii`, column tag `pii` (`email`, `phone`, `name`, `address`) |
| Grants | every workspace user (`account users`) reads `gold`, masked; only the service principal and the catalog owner read `bronze` and `silver` |
| Lineage and audit | Unity Catalog lineage of the pipeline and jobs; the pipeline event log as a table |

## Repository layout

| Path | Content |
|---|---|
| `databricks.yml`, `resources/` | Databricks Asset Bundle: schemas, volume, pipeline `vio_travel_lakehouse`, jobs `vio_travel_pipeline`, `vio_travel_backfill`, `vio_travel_catalog`, `vio_travel_governance`, dashboard, Genie space, SQL alert |
| `src/` | job tasks; `src/lakehouse/` holds the pipeline source, one file per layer |
| `dashboards/build_dashboard.py` | generates `dashboards/vio_travel_overview.lvdash.json`; edit the script, not the JSON |
| `genie/build_genie_space.py` | generates `genie/vio_travel_genie.geniespace.json`; edit the script, not the JSON |
| `local/` | `push_to_bc.py`, `config.json`, systemd units for the push after each job run, `extend_number_series.py` for a Business Central number series that ran out |
| `sql/` | one-time SQL: `setup-catalog.sql`, `migrate-to-lakehouse.sql`, `ref-maps-lab-seed.sql` |
| `tests/` | unit tests for the API client, the snapshot and datasheet readers, the backfill planner and the Business Central client |
| `pyproject.toml`, `.github/workflows/ci.yml` | ruff rules; CI runs ruff, the tests, checks that the dashboard and Genie JSON match their generators, and `databricks bundle validate` when the repository secrets `DATABRICKS_HOST` and `DATABRICKS_TOKEN` exist |

## Run

```bash
# lint, format, tests
uvx ruff check . && uvx ruff format --check .
uv run --no-project --with pytest --with requests pytest -q tests

# deploy to the workspace folder /Workspace/Users/<you>/vio-data-platform/prod, then apply governance, run once
databricks bundle deploy -t prod
databricks bundle run vio_travel_governance -t prod
databricks bundle run vio_travel_pipeline -t prod

# re-extract one day onwards, or a long range month by month
databricks bundle run vio_travel_pipeline -t prod --params backfill_from=2026-11-01
databricks bundle run vio_travel_backfill -t prod --params backfill_from=2015-01-01,backfill_to=2026-11-16

# rebuild silver and gold from bronze (bronze itself is never reset)
databricks pipelines start-update <pipeline_id> --full-refresh

# after a code change in bc_tables, rebuild even though silver did not change
databricks bundle run vio_travel_pipeline -t prod --params rebuild=true

# read the accommodation catalog now instead of on Sunday
databricks bundle run vio_travel_catalog -t prod

# regenerate the dashboard or the Genie space after editing their scripts
python3 dashboards/build_dashboard.py && python3 genie/build_genie_space.py && databricks bundle deploy -t prod

# anomaly detection (freshness and completeness) on a schema; not a bundle resource yet
databricks data-quality create-monitor --json '{"object_type": "schema", "object_id": "<schema_id>", "anomaly_detection_config": {}}'

# local push into Business Central (needs `az login` as the trial owner)
cd local && uv run --no-project --with databricks-sdk --with requests push_to_bc.py --dry-run
cp systemd/*.service systemd/*.timer ~/.config/systemd/user/ && systemctl --user daemon-reload
systemctl --user enable --now vio-push-bc.timer

# when a Business Central number series runs out (trial My Company: P-INV+ ended at 109999); the line starts
# in 2015 because documents take their number by posting date
uv run --no-project --with databricks-sdk --with requests extend_number_series.py \
  --series P-INV+ --line_no 20000 --starting_no 110000 --ending_no 199999 --starting_date 2015-01-01
```

The `dev` target writes to catalog `vio_travel_dev` (create it first with `sql/setup-catalog.sql`, catalog name
changed) and pauses the schedules. Free Edition allows one active pipeline, so stop the prod pipeline before an
update of the dev one.

## Secrets and personal data

Travel Compositor credentials live in the Databricks secret scope `vio-tc`; Business Central uses the Azure CLI
login of the person running `local/push_to_bc.py`. No credential is stored in this repo. Bronze, silver and some gold
tables hold personal data of travellers, users and agencies; raw API files never go into git (`.gitignore`).
