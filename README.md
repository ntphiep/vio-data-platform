# vio-data-platform

Data platform for Vio Travel on Databricks. It pulls bookings, agencies and suppliers from the Travel Compositor API
(Vio's booking platform, operator `easia`), keeps them in the Unity Catalog catalog `vio_travel`, models them as a
star schema for reporting, shapes them for the Business Central API, and shows them in an AI/BI dashboard.

Research notes, field mapping and measurements live outside this repo, in `~/W/viotravel/vio_docs/`
(Vietnamese, starts at `README.md`).

## Flow

| Step | Code | Reads | Writes |
|---|---|---|---|
| `extract_tc` | `src/ingest_tc.py` | Travel Compositor API, GET only | volume `raw.landing`, gzip NDJSON, one file per page |
| `bronze_silver` | `src/bronze_silver.py` | `raw.landing` | `bronze.tc_*`, then `silver.*` by `MERGE` |
| `star_schema` | `src/star_schema.py` | `silver.*`, `ref.umbrella_agency` | `gold.dim_*`, `gold.fact_*`, `gold.obt_booking_service` |
| `bc_tables` | `src/bc_tables.py` | `silver.*`, `ref.*` | `business_central.*`, then a row in `ops.publish` |
| `data_quality` | `src/data_quality.py` | `ops.*`, `silver.bookings`, `business_central.sales_invoices` | `ops.dq_results`, end-to-end checks |
| dashboard | `dashboards/vio_travel_overview.lvdash.json` | `gold.*`, `business_central.*`, `ops.*` | AI/BI dashboard Vio Travel overview |
| push | `local/push_to_bc.py` | `business_central.*` at the published versions | Business Central, company My Company; `ops.bc_sync` |
| alert | `resources/pipeline_freshness.alert.yml` | `ops.dq_results` | e-mail when no end-to-end check arrived for 75 minutes |

The five tasks run in the Databricks job `vio_travel_pipeline` every 30 minutes, at minute 05 and 35
(Asia/Ho_Chi_Minh). `star_schema` and `bc_tables` depend only on `bronze_silver`, run in parallel, and skip the
rebuild when silver and ref have not changed since their last build (content token in `ops.build_state`), so runs
without new bookings stay short on the Free Edition compute quota. `data_quality` runs last, also after a failed
task. The push into Business Central runs on a local machine because it uses a delegated Azure CLI login, which a
job cannot do; its systemd timer asks the Jobs API every 5 minutes and pushes after each finished job run.

## Freshness

| Stage | Up to date within | Watched by |
|---|---|---|
| Travel Compositor API to silver | 30 minutes plus the run time, about 4 minutes | `e2e_extract_failing_or_stale`, alert after 75 minutes |
| silver to gold and the dashboard | same run; the dashboard queries gold live | `e2e_gold_behind_silver` |
| silver to `business_central` | same run | `e2e_business_central_behind_silver` |
| `business_central` to Business Central | 5 minutes after the run, 100 bookings per push | `e2e_push_stalled`, `e2e_sales_waiting_for_push` |
| the job itself | | SQL alert `pipeline_freshness`; anomaly detection of Unity Catalog on schemas `silver`, `gold`, `business_central` |

The Travel Compositor API filters bookings by update date only to the day (`ufrom`), so a run re-reads today and
yesterday. Webhooks would need a URL registered on Vio's Travel Compositor account, which is out of scope.

## Layers in catalog `vio_travel`

| Schema | Layer | What it holds |
|---|---|---|
| `raw` | landing | volume `landing`: API responses as returned |
| `bronze` | raw history | `tc_bookings`, `tc_agencies`, `tc_suppliers`, one VARIANT row per landed record, append-only |
| `silver` | cleaned current state | `bookings`, `booking_services`, `agencies`, `suppliers` |
| `gold` | analytics model | star schema and one big table, below |
| `business_central` | integration output | `customers`, `vendors`, `sales_invoices`, `sales_lines`, `purchase_invoices`, `purchase_lines` |
| `ref` | mappings | `umbrella_agency`, `customer_map`, `vendor_map`, maintained by Vio Finance |
| `ops` | operations | `api_calls`, `ingest_runs`, `watermark`, `silver_progress`, `build_state`, `dq_results`, `publish`, `bc_sync`; volume `checkpoints` |

## Data model (schema `gold`)

Kimball star schema with type 1 dimensions. Travel Compositor returns the current state of agencies and suppliers,
not their history, so type 2 history would have nothing to track yet.

| Table | Grain | Keys and main columns |
|---|---|---|
| `dim_date` | one calendar day, 2015 to 2030 | `date_key` (yyyyMMdd), year, quarter, month, `year_month`, week, weekday |
| `dim_agency` | one agency | `agency_key` (Travel Compositor agency ID), name, country, `is_umbrella`; `-1` = unknown |
| `dim_supplier` | one supplier | `supplier_key`: `SP<id>` Travel Compositor supplier, `PV<code>` provider, `MN:<name>` manual service; `-1` = unknown |
| `dim_service_type` | one service type | `service_type` (`hotelservice`, `manual_hotel`, ...), `service_category`, `is_manual` |
| `fact_booking` | one booking, current state | `booking_ref`, `agency_key`, `created_date_key`, `start_date_key`, `end_date_key`; status, trip type; `total_price`, `net_provider`, `gross_margin`, pax |
| `fact_booking_service` | one service of a booking | `service_key`, `booking_ref`, `supplier_key`, `agency_key`, dates; destination country; sale and cost amounts, cost in the booking currency |
| `obt_booking_service` | one service of a booking | `fact_booking_service` joined with every dimension and booking attribute, for ad-hoc SQL and Genie |

`SP` and `PV` keys of `supplier_key` equal the vendor numbers in `business_central.vendors`, unless Vio Finance
maps a provider to another vendor in `ref.vendor_map`. Manual suppliers keep their name (`MN:<name>`) in gold and
get a vendor number only through `ref.vendor_map`.

## Reliability and data quality

| Concern | How |
|---|---|
| API errors | up to 6 attempts on 429, 5xx, network errors and truncated JSON, full-jitter backoff capped at 120 s, `Retry-After` honoured, token renewed 5 minutes before expiry (`src/ingest_tc.py`) |
| Missed bookings | 7-day windows, distinct `bookingReference` compared with `totalResults`, one retry, then status PARTIAL and the watermark does not move |
| Exactly-once load | Auto Loader checkpoints in volume `ops.checkpoints`; files written as `.tmp` then renamed |
| Out-of-order updates | `MERGE` into `silver.bookings` only when `lastUpdateDate` is newer; services replaced in one atomic `MERGE` |
| Consistent reads | write-audit-publish: `star_schema` builds temporary views, checks them, then replaces the gold tables; `bc_tables` records table versions in `ops.publish` only after its blocking checks pass, and `local/push_to_bc.py` reads `VERSION AS OF` those versions |
| Checks | `ops.dq_results`; the check name starts with the layer it tests (`bronze_`, `silver_`, `gold_`, `bc_`, `e2e_`); block stops the replace or publish, warn is recorded, alert (only in `data_quality`) fails the task so the failure e-mail goes out |
| Business Central writes | POST retried only on 429; documents matched by `externalDocumentNumber` and `vendorInvoiceNumber`; 3 failures with the same hash stop retries; results flushed per booking |
| Replay | job parameters `backfill_from=yyyy-mm-dd` (re-extract) and `full_refresh=true` (rebuild bronze and silver from the volume) |
| Operations | one run at a time with queueing, e-mail on failure and on runs over 30 minutes, tag `contains_pii=true` on tables with personal data, predictive optimization on the catalog |

## Repository layout

| Path | Content |
|---|---|
| `databricks.yml`, `resources/` | Databricks Asset Bundle: schemas, volumes, job `vio_travel_pipeline`, dashboard `vio_travel_overview`, SQL alert `pipeline_freshness` |
| `src/` | job tasks |
| `dashboards/build_dashboard.py` | generates `dashboards/vio_travel_overview.lvdash.json`; edit the script, not the JSON |
| `local/` | `push_to_bc.py`, `config.json`, systemd units for the push after each job run |
| `sql/` | one-time SQL: `setup-catalog.sql`, `ref-maps-lab-seed.sql` |
| `tests/` | unit tests for the API client and the Business Central client |
| `pyproject.toml`, `.github/workflows/ci.yml` | ruff lint and format rules; CI runs ruff, the tests, a check that the dashboard JSON matches its generator, and `databricks bundle validate` when the repository secrets `DATABRICKS_HOST` and `DATABRICKS_TOKEN` exist |

## Run

```bash
# lint, format, tests
uvx ruff check . && uvx ruff format --check .
uv run --no-project --with pytest --with requests pytest -q tests

# deploy to the workspace folder /Workspace/Users/<you>/vio-data-platform/prod and run once
databricks bundle deploy -t prod
databricks bundle run vio_travel_pipeline -t prod

# after a code change in star_schema or bc_tables, rebuild even though silver did not change
databricks bundle run vio_travel_pipeline -t prod --params rebuild=true

# re-extract from a day, or rebuild bronze and silver from the landed files
databricks bundle run vio_travel_pipeline -t prod --params backfill_from=2026-11-01
databricks bundle run vio_travel_pipeline -t prod --params full_refresh=true

# anomaly detection (freshness and completeness) on a schema; not a bundle resource yet
databricks data-quality create-monitor --json '{"object_type": "schema", "object_id": "<schema_id>", "anomaly_detection_config": {}}'

# regenerate the dashboard after editing dashboards/build_dashboard.py
python3 dashboards/build_dashboard.py && databricks bundle deploy -t prod

# local push into Business Central (needs `az login` as the trial owner)
cd local && uv run --no-project --with databricks-sdk --with requests push_to_bc.py --dry-run
cp systemd/*.service systemd/*.timer ~/.config/systemd/user/ && systemctl --user daemon-reload
systemctl --user enable --now vio-push-bc.timer
```

The `dev` target writes to catalog `vio_travel_dev` (create it first with `sql/setup-catalog.sql`, catalog name
changed) and pauses the schedule.

## Secrets and personal data

Travel Compositor credentials live in the Databricks secret scope `vio-tc`; Business Central uses the Azure CLI
login of the person running `local/push_to_bc.py`. No credential is stored in this repo. Bronze, silver and some gold
tables hold personal data of travellers and agencies; raw API files never go into git (`.gitignore`).
