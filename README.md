# vio-data-platform

Lakehouse on Databricks for Vio Travel, a destination management company whose bookings live in Travel Compositor
(operator `easia`). The platform reads the Travel Compositor REST API, keeps every record and its history in the Unity
Catalog catalog `vio_travel`, models it as a star schema for an AI/BI dashboard and a Genie space, and turns bookings
into sales and purchase invoices in Microsoft Dynamics 365 Business Central.

It is a lab: it runs in a Databricks Free Edition workspace and writes into the company `My Company` of a Business
Central trial. The Travel Compositor API is only read (GET, plus the authenticate POST).

![Data flow](docs/data-flow.png)

## Contents

- [Architecture](#architecture)
- [Unity Catalog layout](#unity-catalog-layout)
- [Ingestion](#ingestion)
- [Transformation](#transformation)
- [Data model](#data-model)
- [Business Central integration](#business-central-integration)
- [Orchestration](#orchestration)
- [Data quality](#data-quality)
- [Monitoring and freshness](#monitoring-and-freshness)
- [Performance and cost](#performance-and-cost)
- [Governance and security](#governance-and-security)
- [Conventions](#conventions)
- [Development](#development)
- [Operations](#operations)
- [Limitations](#limitations)

## Architecture

| Stage | Runs as | Code | Output |
|---|---|---|---|
| Extract | task `extract_travel_compositor` of job `vio_travel_ingest_transform_publish` | `src/extract_travel_compositor.py` | gzip NDJSON files in volume `raw.landing`, run log in `ops` |
| Bronze, silver, gold | Lakeflow pipeline `vio_travel_build_bronze_silver_gold`, task `build_bronze_silver_gold` | `src/build_bronze_silver_gold/bronze.py`, `silver.py`, `gold.py` | schemas `bronze`, `silver`, `gold` |
| Integration tables | task `build_business_central_tables` | `src/build_business_central_tables.py` | schema `business_central`, published version in `ops.publish` |
| Checks | task `check_data_quality` | `src/check_data_quality.py` | `ops.dq_results` |
| Push | systemd timer `vio-push-business-central.timer` on a workstation | `local/push_to_business_central.py` | Business Central draft invoices, `ops.business_central_sync` |
| Serving | AI/BI dashboard `Vio Travel overview`, Genie space `Vio Travel Genie`, 3 metric views | `dashboards/`, `genie/`, `src/apply_governance.py` | |

The design is ELT: API responses are landed unchanged, and every transformation runs inside Databricks, so a parsing
bug is fixed by replaying landed files, not by calling the API again.

Repository layout:

| Path | Content |
|---|---|
| `databricks.yml`, `resources/` | Databricks Asset Bundle: schemas, volume, pipeline, 4 jobs, dashboard, Genie space, SQL alert |
| `src/` | job tasks; `src/build_bronze_silver_gold/` is the pipeline source, one file per layer |
| `dashboards/build_dashboard.py`, `genie/build_genie_space.py` | generators of the dashboard and Genie JSON; edit the script, never the JSON |
| `local/` | the Business Central push, its config, systemd units, `extend_number_series.py` |
| `sql/` | one-time setup: `setup-catalog.sql` (catalog, grants, masking function, `ref` tables), `ref-maps-lab-seed.sql` (lab-only mappings, never in production) |
| `tests/` | unit tests of the API client, readers, backfill planner, push logic and shared SQL fragments |
| `docs/` | `data-flow.drawio` and `star-schema.drawio` with their PNG exports |
| `.github/workflows/ci.yml`, `.githooks/pre-push` | the same checks in GitHub Actions and before every local push |

## Unity Catalog layout

One catalog per environment: `vio_travel` (target `prod`) and `vio_travel_dev` (target `dev`). Schemas follow the
medallion layers plus integration, reference and operations schemas.

| Schema | Holds | Written by | Readers |
|---|---|---|---|
| `raw` | volume `landing`: one folder per entity, ingest date and run; files kept 90 days | `extract_travel_compositor` | pipeline |
| `bronze` | `tc_<entity>`: one VARIANT row per landed record, append-only, full history | pipeline (Auto Loader) | pipeline |
| `silver` | current state per entity and `<entity>_history` (SCD type 2); typed columns, personal data masked | pipeline | `build_business_central_tables`, analysts with grant |
| `gold` | star schema `dim_*`, `fact_*`, one big table `obt_booking_service`, metric views `*_metrics` | pipeline, `vio_travel_apply_governance` | every workspace user, masked |
| `business_central` | `customers`, `vendors`, `sales_invoices`, `sales_lines`, `purchase_invoices`, `purchase_lines` in the shape of the Business Central API v2.0 | `build_business_central_tables` | `local/push_to_business_central.py` |
| `ref` | `umbrella_agency`, `customer_map`, `vendor_map`: mappings owned by Vio Finance | Finance, through SQL | `build_business_central_tables`, pipeline |
| `governance` | function `mask_pii` | `vio_travel_apply_governance` | column masks |
| `ops` | `api_calls`, `ingest_runs`, `watermark`, `accommodation_fetch`, `pipeline_event_log`, `build_state`, `dq_results`, `publish`, `business_central_sync` | every task and the push | dashboard page Pipeline operations, alert |

Schemas, the volume and the pipeline event log are bundle resources (`resources/schemas.yml`); schemas that hold
history have `prevent_destroy`. The catalog itself is created once by `sql/setup-catalog.sql`, because a catalog on
Default Storage cannot be created through the catalogs API.

## Ingestion

`src/extract_travel_compositor.py` declares every entity in one registry and reads it in one of three ways:

| Mode | Entities | Strategy |
|---|---|---|
| incremental | bookings (`/booking/getBookings`) | by last-update date from the watermark in `ops.watermark`, 1-day overlap, 7-day windows; the API filters by day only, so each run reads today and yesterday again |
| snapshot | agencies, suppliers, users, travel ideas (every 6 hours); pending payments, purchase attempts (every run); accommodation catalog, 283 pages (weekly, job `vio_travel_ingest_accommodation_catalog`, 4 parallel requests) | every page of the list; the pipeline compares consecutive snapshots |
| by id | accommodation datasheets of booked hotels | 200 per run, refreshed after 30 days |

Reliability of the extract:

- Retries with exponential backoff and full jitter on 429, 5xx, network errors and truncated JSON, up to 6 attempts,
  capped at 120 seconds, honouring `Retry-After`. The token is renewed 5 minutes before expiry and shared by
  parallel readers.
- Completeness: the distinct keys landed are compared with `totalResults` for every window and snapshot. A short
  booking window is read again and then recorded as `PARTIAL`, and the watermark stays. A short or empty snapshot is
  never applied, because the pipeline would close every key missing from it.
- Atomic files: each API page is written as `.tmp` and renamed, so Auto Loader never reads a half-written file.
- Every HTTP attempt is logged in `ops.api_calls`, every window and snapshot in `ops.ingest_runs`.

Webhooks are not used: they need a URL registered on Vio's Travel Compositor account, which is a write to the
customer's system. Contracts and `/manual-exchange-rates` (401 for Vio's API user) are not read.

## Transformation

The Lakeflow Spark Declarative Pipeline `vio_travel_build_bronze_silver_gold` (serverless, triggered, channel `CURRENT`) builds all
three layers. Its event log is the table `ops.pipeline_event_log`.

| Layer | Technique |
|---|---|
| bronze | Auto Loader (`cloudFiles`) streaming tables, one per entity, VARIANT payload plus file and run metadata; exactly-once by file; `pipelines.reset.allowed = false`, so a full refresh never empties them |
| silver, bookings | `AUTO CDC` keyed on `booking_ref`, sequenced by `lastUpdateDate` then load time: SCD type 1 in `bookings` and SCD type 2 of status and amounts in `bookings_history`; late or replayed versions never overwrite newer ones |
| silver, snapshots | `AUTO CDC FROM SNAPSHOT` per entity: SCD type 2 in `<entity>_history`, which also closes keys that disappear (a paid pending payment), and the current rows in `<entity>` |
| silver, children | materialized views exploding the booking payload: `booking_services` (one row per service, all service arrays), `booking_payments`; `booking_invoices`, `booking_price_components` and `accommodations` are plain views to save table quota |
| gold | materialized views, refreshed incrementally on serverless when the query plan allows |

Incremental processing end to end: the extract reads only changed bookings, Auto Loader picks up only new files, the
streaming tables process only new bronze rows, and `build_business_central_tables` skips its rebuild when the silver tables did not change
since the last publish (`ops.build_state`).

A pipeline full refresh rebuilds silver and gold from bronze; bronze and the snapshot history tables are excluded
because their history exists only there.

## Data model

![Star schema](docs/star-schema.png)

Kimball star schema in `gold`. Dimensions are SCD type 1 with member `-1` (Unknown) for missing keys;
`dim_agency_history` gives the type 2 view for as-of questions. Facts and dimensions carry informational primary and
foreign keys with `RELY`, which the optimizer and Genie use for joins.

| Table | Grain |
|---|---|
| `dim_date` | one calendar day, 2015 to 2030, key `date_key` (yyyyMMdd) |
| `dim_agency`, `dim_agency_history` | one agency; one agency version |
| `dim_supplier` | one supplier: `SP<id>` Travel Compositor supplier, `PV<code>` provider, `MN:<name>` manual service; `SP` and `PV` keys equal the Business Central vendor numbers |
| `dim_service_type` | one service type with category |
| `dim_user` | one microsite user, key SHA-256 of agency and username |
| `dim_travel_idea` | one travel idea |
| `dim_accommodation` | one booked hotel |
| `fact_booking` | one booking, current state |
| `fact_booking_service` | one service of a booking, sale and cost amounts |
| `fact_booking_payment` | one payment received on a booking |
| `fact_pending_payment` | one payment due, with due date; days overdue are computed at query time by `receivable_metrics`, so they never freeze when the pipeline stops |
| `fact_booking_status_change` | one tracked change of status or amount, from `silver.bookings_history` |
| `fact_purchase_attempt` | one checkout started, converted or not |
| `obt_booking_service` | `fact_booking_service` joined with every dimension, for ad-hoc SQL and Genie |

Metric views (`booking_metrics`, `service_metrics`, `receivable_metrics`, YAML 1.1) define revenue, margin,
cancellation rate, service cost by supplier and amounts due or overdue once, for the dashboard, Genie and SQL users.
Amounts are in the booking currency and every metric view has a `Currency` dimension: bookings are in USD except a
few EUR bookings with total 0, so totals are never summed across currencies.

The dashboard `Vio Travel overview` has 5 pages: Bookings and revenue, Suppliers and cost, Payments due, Ideas,
hotels and checkouts, Pipeline operations. The Genie space answers questions on the gold tables and metric views with
the asking user's permissions.

## Business Central integration

Business Central is the accounting system (ERP). The integration creates draft documents that Finance reviews and
posts; it never posts.

| Business Central object | From | Key |
|---|---|---|
| customer | Travel Compositor agency, or the customer that `ref.customer_map` assigns to a booking of an umbrella agency | `AG<agency id>` |
| vendor | Travel Compositor supplier, provider code of an API service, or `ref.vendor_map` for manual services | `SP<id>`, `PV<code>` |
| sales invoice | one per booking in status BOOKED, RQ or CANCELED; one line per active service | `externalDocumentNumber` = booking reference |
| purchase invoice | one per service with a cost | `vendorInvoiceNumber` = service id and type code |
| currency exchange rate | ECB rate (Frankfurter API); for currencies the ECB does not publish (VND, FJD), the median Travel Compositor cost rate of at least 3 nearby services | currency and date |

`build_business_central_tables` shapes these rows, runs its blocking checks, and only then records the Delta versions of the six tables in
`ops.publish` (write-audit-publish). A document that cannot be sent gets a `hold_reason`, for example
`CUSTOMER_UNMAPPED` or `VENDOR_UNMAPPED`, and waits for a mapping.

`local/push_to_business_central.py` runs on a workstation because Business Central accepts only a delegated Azure CLI login there
(no app registration in the tenant). Each run:

1. Asks the Jobs API whether `vio_travel_ingest_transform_publish` finished a run since the last push (timer every 5 minutes).
2. Reads the six tables `VERSION AS OF` the last published versions, so it sees one consistent, checked build.
3. Compares each document's `payload_hash` with its last result in `ops.business_central_sync` and selects up to 100 bookings with
   changes, oldest first.
4. Upserts customers and vendors, loads missing exchange rates through a RapidStart configuration package.
5. Creates a draft invoice with its lines in one deep-insert POST, or replaces the header and lines of an
   existing draft in one transactional `$batch`. A posted document is never changed and is reported as
   `NEEDS_CREDIT_MEMO`; a cancelled booking empties its draft so it cannot be posted. Drafts are never deleted,
   because deleting a numbered draft makes Business Central create an empty posted invoice.
6. Writes one result row per document into `ops.business_central_sync` after every booking.

Retries: POST is retried only on 429, because a POST that failed on 5xx may have been committed; the next run finds
the document by its key and updates it instead of creating a duplicate. A document that fails 3 times with the same
hash waits as a dead letter until its data changes or `--retry FAILED` is used.

### Fields prepared for the YVS specification

YVS, the partner that implements Business Central for Vio, specifies the integration in the files
`VIO_Data warhouse_Integration mapping fields-V1.2.xlsx` and `VIO_SO PO integration scenarios-V1.2.xlsx`. The
`business_central` tables already carry every field of those specifications that Travel Compositor provides; the push
sends only the fields of the standard API v2.0 until YVS publishes its endpoints and dimensions.

| Specification field | Column | Source in Travel Compositor |
|---|---|---|
| MAIN BOOKING dimension (Main EXA) | `sales_invoices.main_booking_ref` | `bookingAssociatedTo`, else the booking itself |
| Your Reference | `sales_invoices.your_reference` | `referralId` |
| Sales channel, brand (microsite), trip type, service end date, total passengers | `sales_channel`, `microsite`, `trip_type`, `service_end_date`, `pax` | booking |
| Salesperson | `sales_invoices.salesperson_username` | `user.username` of the booking |
| Agency management fee | `sales_invoices.agency_management_fee` | `pricebreakdown.agencyManagementFee` |
| Deposit and balance lines, balance due date | `deposit_amount`, `balance_amount`, `balance_due_date` | `payment.amount`, `payment.deferredAmount`, `payment.deferredLimitDate` |
| Cancellation date | `sales_invoices.cancellation_date`, `purchase_invoices.cancellation_date` | service `cancelationDate` |
| Destination country dimension | `sales_invoices.destination_country` | country of the first service |
| Supplier confirmation number | `purchase_invoices.supplier_confirmation` | service `bookingReference` |
| Currency factor | `purchase_invoices.currency_factor` | service `pricebreakdown.exchangeRateMicrosite` |
| Amendments | `purchase_invoices.amendment_count`, view `silver.booking_service_amendments` | service `amendments` |
| Customer phone, VAT registration, credit limit, deferred terms | `customers.phone_number`, `tax_registration_number`, `credit_limit`, `deferred_days` | agency |
| Vendor phone | `vendors.phone_number` | supplier |

Data the specifications ask for that Travel Compositor does not hold, so Vio or YVS must provide it: agent currency
and its exchange rate, business unit per user, supplier payment terms, vendor invoice numbers, item per service type,
TCODE (credit or refund reason), invoice number series, payment requests, and approved customer and vendor mappings
(today 7,241 sales invoices wait as `CUSTOMER_UNMAPPED` and 23,656 purchase invoices as `VENDOR_UNMAPPED`).

## Orchestration

| Job or unit | Schedule (Asia/Ho_Chi_Minh) | Tasks | Run as |
|---|---|---|---|
| `vio_travel_ingest_transform_publish` | every 30 minutes at minute 05 and 35 | `extract_travel_compositor` → `build_bronze_silver_gold` → `build_business_central_tables` → `check_data_quality` (`run_if: ALL_DONE`) | service principal `vio-travel-pipeline` |
| `vio_travel_ingest_accommodation_catalog` | Sunday 03:15 | `extract_accommodation_catalog`: accommodation catalog snapshot | service principal |
| `vio_travel_apply_governance` | Sunday 03:45, and after each deploy | tags, masks, grants, comments, metric views | deploying user (catalog owner) |
| `vio_travel_backfill_bookings` | manual | `plan_backfill_windows` → for-each `extract_booking_windows` (one month per iteration, 3 in parallel) | service principal |
| `vio-push-business-central.timer` (systemd) | every 5 minutes, acts after each finished job run | `push_to_business_central.py --on-new-run` under `flock` | the Business Central user |
| SQL alert `pipeline_freshness` | hourly at minute 50 | dead-man switch on `ops.dq_results` | deploying user |

Retries and limits: `extract_travel_compositor` 2 retries 5 minutes apart; the pipeline retries an update once itself
(`pipelines.numUpdateRetryAttempts`), so its task has no retry; `build_business_central_tables` 1 retry; job timeout 2 hours; one active
run per job, later runs queue.

The backfill job only lands files and never moves the watermark; the next scheduled run loads them, so the two jobs
never update the pipeline or rebuild `business_central` at the same time. A failed month is re-read with "Repair
run".

## Data quality

| Where | What | Effect |
|---|---|---|
| pipeline expectations | keys present, dates ordered, amounts present, payload parsed (`bronze_`, `silver_`) | rows without a key are dropped, other rules warn; counts copied into `ops.dq_results` |
| `build_business_central_tables` checks | unique document keys, every document has a customer or vendor and a currency, one currency per invoice, Business Central field lengths | severity `block`: nothing is published, the push keeps the previous build |
| `build_business_central_tables` and `check_data_quality` warnings | held documents, totals that differ from lines, unknown service arrays, orphan facts, overdue payments | severity `warn`: recorded and shown on the dashboard |
| `check_data_quality` end-to-end checks (`e2e_`) | extract failing or stale per entity, pipeline failed or stale, `business_central` behind silver, push stalled | severity `alert`: the task fails and the failure e-mail goes out |

Every check writes one row per run into `ops.dq_results` with the number of failing rows and up to 5 sample keys. A
check name starts with the layer or step it tests: `bronze_`, `silver_`, `gold_`, `business_central_`, `e2e_`.

## Monitoring and freshness

| Stage | Fresh within | Watched by |
|---|---|---|
| Travel Compositor to silver | 30 minutes plus run time | `e2e_extract_failing_or_stale`, `e2e_pipeline_failed_or_stale` |
| silver to gold, dashboard, Genie | same pipeline update | expectations, `gold_` checks |
| silver to `business_central` | same job run | `e2e_business_central_behind_silver` |
| `business_central` to Business Central | 5 minutes after the run, 100 bookings per push | `e2e_push_stalled`, `e2e_sales_waiting_for_push` |
| the job itself | | SQL alert `pipeline_freshness` (no `e2e_` row for 75 minutes, evaluated hourly, so up to about 2 hours to detect) |

Notifications: job e-mail on failure and on runs over 30 minutes (`health` rule), pipeline e-mail on update and flow
failure, desktop notification when the push unit fails. The dashboard page Pipeline operations shows run times, API
calls, check results and the push backlog from `ops`.

## Performance and cost

- Layout: liquid clustering (`CLUSTER BY` on join keys of facts, `CLUSTER BY AUTO` elsewhere) instead of partitions
  and Z-ORDER; predictive optimization on the catalog runs OPTIMIZE, VACUUM and statistics.
- Silver streaming tables have change data feed and row tracking; bronze has deletion vectors and row tracking.
- Serverless compute everywhere; the pipeline is triggered, not continuous, because the source changes at most every
  30 minutes and Free Edition has a daily compute quota.
- Free Edition limits shape the design: one active pipeline, at most 5 concurrent tasks, 500 tables per metastore.
  Each pipeline table counts twice (its hidden `__materialization_*` table), so datasets read by nothing inside the
  pipeline are plain views created by `vio_travel_apply_governance`.
- Retention: landing files 90 days; `ops.api_calls` and `ops.dq_results` 180 days; bronze, history tables,
  `ops.ingest_runs`, `ops.publish` and `ops.business_central_sync` are kept as audit trail.

## Governance and security

| Control | Implementation |
|---|---|
| Identity | jobs and the pipeline run as the service principal `vio-travel-pipeline`; nobody runs production code as a person except `vio_travel_apply_governance` and the push |
| Secrets | Travel Compositor credentials in the secret scope `vio-tc`, readable only by the service principal and the owner; no credential in the repo |
| Personal data | column tag `pii` (`email`, `phone`, `name`, `address`) and table tag `contains_pii`; column masks with `governance.mask_pii`, declared in the pipeline schemas of silver and `gold.dim_user`, show values only to the group `vio_pii_readers`; `business_central` is not masked because the push must send real e-mails and addresses, and grants keep it to the service principal and the catalog owner |
| Access | every workspace user (`account users`) reads `gold`, masked; only the service principal and the catalog owner read `bronze`, `silver` and `business_central` |
| Documentation | comments on every silver and gold column (pipeline schemas) and on every table, including `ops`, `ref` and `business_central`; Catalog Explorer and Genie read them |
| Lineage and audit | Unity Catalog lineage across pipeline and jobs; pipeline event log, `ops.ingest_runs`, `ops.publish`, `ops.business_central_sync` |

Raw API files and anything under `data/` never go into git (`.gitignore`).

## Conventions

| Item | Convention | Example |
|---|---|---|
| Catalog, schema, table, column | snake_case, English, plural entity names in silver | `silver.booking_services`, `booking_ref` |
| Gold tables | `dim_`, `fact_`, `obt_` prefixes; metric views end in `_metrics` | `fact_pending_payment` |
| History tables | `<entity>_history` with `__START_AT`, `__END_AT` | `silver.agencies_history` |
| Bronze tables | `tc_<entity>`, source system prefix | `bronze.tc_bookings` |
| Jobs and pipelines | `vio_travel_<verb>_<object>`: the name says what it does, the schedule says when; file `<name>.<type>.yml` | `vio_travel_ingest_transform_publish`, `resources/vio_travel_ingest_transform_publish.job.yml` |
| Task keys | `<verb>_<object>`, source file or folder named like the task | task `extract_travel_compositor`, file `src/extract_travel_compositor.py`; task `build_bronze_silver_gold`, folder `src/build_bronze_silver_gold/` |
| Check names | `<layer or schema>_<what is wrong>` | `gold_fact_orphans`, `business_central_sales_key_unique` |
| Tags | job and pipeline tag `project: vio_travel`, `contains_pii: "true"` where personal data is read; UC tags `contains_pii`, `pii` | |
| Python | ruff lint and format, line length 120, standard library plus `requests` and `databricks-sdk` | |
| Commits | imperative English subject, one concern per commit | `Retry the rate source on timeouts and 5xx` |
| Branches | `main` is deployed; work on `<user>-dev/<topic>` and merge | `harry-dev/lakehouse` |

## Development

Requirements: Python 3.11 or later, [uv](https://docs.astral.sh/uv/), the Databricks CLI (tested with v1.22.1)
logged in to the workspace, and the Azure CLI for the push.

```bash
uvx ruff check . && uvx ruff format --check .
uv run --no-project --with pytest --with requests --with databricks-sdk pytest -q tests
git config core.hooksPath .githooks        # run the CI checks before every git push
databricks bundle validate -t prod
```

Targets: `dev` deploys to catalog `vio_travel_dev` with `mode: development` (resource names prefixed, schedules
paused); `prod` deploys to `vio_travel`. Free Edition allows one active pipeline, so stop the prod pipeline before
updating the dev one. To create a new catalog, run `sql/setup-catalog.sql` with the catalog name replaced.

CI (`.github/workflows/ci.yml`) runs ruff, the tests, checks that the dashboard and Genie JSON match their
generators, and `databricks bundle validate` when the repository secrets `DATABRICKS_HOST` and `DATABRICKS_TOKEN`
exist. `.githooks/pre-push` runs the same checks locally.

## Operations

```bash
# deploy, apply governance, run once
databricks bundle deploy -t prod
databricks bundle run vio_travel_apply_governance -t prod
databricks bundle run vio_travel_ingest_transform_publish -t prod

# re-extract from a day onwards (moves the watermark), or a long range month by month (does not)
databricks bundle run vio_travel_ingest_transform_publish -t prod --params backfill_from=2026-11-01
databricks bundle run vio_travel_backfill_bookings -t prod --params backfill_from=2015-01-01,backfill_to=2026-11-16

# rebuild silver and gold from bronze; bronze and snapshot history are kept
databricks pipelines start-update <pipeline_id> --full-refresh

# rebuild business_central after a code change in build_business_central_tables although silver did not change
databricks bundle run vio_travel_ingest_transform_publish -t prod --params rebuild=true

# regenerate the dashboard or Genie space after editing their scripts
python3 dashboards/build_dashboard.py && python3 genie/build_genie_space.py && databricks bundle deploy -t prod

# Business Central push (needs `az login` as a Business Central user)
cd local
uv run --no-project --with databricks-sdk --with requests push_to_business_central.py --dry-run
cp systemd/*.service systemd/*.timer ~/.config/systemd/user/ && systemctl --user daemon-reload
systemctl --user enable --now vio-push-business-central.timer
uv run --no-project --with databricks-sdk --with requests push_to_business_central.py --retry SKIPPED,FAILED

# a Business Central number series that ran out (documents take their number by posting date, so start in 2015)
uv run --no-project --with databricks-sdk --with requests extend_number_series.py \
  --series P-INV+ --line_no 20000 --starting_no 110000 --ending_no 199999 --starting_date 2015-01-01
```

`local/config.json` holds the target company, G/L accounts, posting groups, the push scope and limits; it contains
IDs only, no credential.

## Limitations

- The Travel Compositor API filters bookings by update day only, so changes arrive with the next 30-minute run, not in
  real time.
- Exchange rates come from the ECB and from Travel Compositor costs, not from the rates Vio Finance uses.
- `ref.customer_map` and `ref.vendor_map` hold lab seed rows that Vio Finance has not approved.
- Posting, credit memos, deposits and the business unit dimension are not implemented.
- The push depends on a workstation and a delegated login; production needs an app registration and a job.
